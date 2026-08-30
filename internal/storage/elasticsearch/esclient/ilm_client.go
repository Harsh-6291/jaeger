// Copyright (c) 2021 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package esclient

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v7"
	"go.uber.org/zap"
)

const (
	maxTries       = 3
	maxElapsedTime = 5 * time.Second
)

var _ IndexManagementLifecycleAPI = (*ILMClient)(nil)

//go:embed lifecycle_policies/*.json
var lifecyclePoliciesFS embed.FS

// ILMClient is a client used to manipulate Index lifecycle management policies.
// It supports both Elasticsearch ILM and OpenSearch ISM APIs, selecting the
// endpoint from the backend version the embedded Client was constructed with.
type ILMClient struct {
	*Client
	MasterTimeoutSeconds int
	Logger               *zap.Logger
}

// policyEndpoint returns the lifecycle-policy API path for name: OpenSearch ISM
// (_plugins/_ism/policies) on OpenSearch, Elasticsearch ILM (_ilm/policy)
// otherwise. Exists and the TestsOnly policy helpers all route through here so
// the ILM-vs-ISM endpoint choice lives in one place.
func (i ILMClient) policyEndpoint(name string) string {
	if i.version.IsOpenSearch() {
		return "_plugins/_ism/policies/" + name
	}
	return "_ilm/policy/" + name
}

// CreatePolicy installs a lifecycle policy (ILM on Elasticsearch, ISM on
// OpenSearch). The body's schema is backend-specific, so the caller supplies it;
// the endpoint is chosen here.
//
// It goes through Perform rather than request() because a successful policy PUT
// is 200 on Elasticsearch but 201 on OpenSearch (and re-creating an ISM policy
// returns 409); request() accepts only 200, so it would reject the 201 and 409.
func (i ILMClient) CreatePolicy(ctx context.Context, name, body string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "/"+i.policyEndpoint(name), strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := i.Perform(req)
	if err != nil {
		return fmt.Errorf("failed to create lifecycle policy %q: %w", name, err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusConflict:
		return nil
	default:
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create lifecycle policy %q: status %d: %s", name, resp.StatusCode, data)
	}
}

// DefaultPolicy returns the default JSON policy body for the client's backend (ILM or ISM).
func (i ILMClient) DefaultPolicy() ([]byte, error) {
	if i.version.IsOpenSearch() {
		return lifecyclePoliciesFS.ReadFile("lifecycle_policies/ism-default.json")
	}
	return lifecyclePoliciesFS.ReadFile("lifecycle_policies/ilm-default.json")
}

// TestsOnlyDeletePolicy removes a lifecycle policy, tolerating a missing one.
// Integration-test-only, same rationale as CreatePolicy.
func (i ILMClient) TestsOnlyDeletePolicy(ctx context.Context, name string) error {
	_, err := i.request(ctx, elasticRequest{
		endpoint: i.policyEndpoint(name),
		method:   http.MethodDelete,
	})
	if err != nil {
		var responseError ResponseError
		if errors.As(err, &responseError) && responseError.StatusCode == http.StatusNotFound {
			return nil
		}
		return fmt.Errorf("failed to delete lifecycle policy %q: %w", name, err)
	}
	return nil
}

// Exists verify if a ILM/ISM policy exists
func (i ILMClient) Exists(ctx context.Context, name string) (bool, error) {
	endpoint := i.policyEndpoint(name)
	operation := func() ([]byte, error) {
		bytes, err := i.request(ctx, elasticRequest{
			endpoint: endpoint,
			method:   http.MethodGet,
		})
		if err != nil {
			i.Logger.Warn(
				"Retryable error while getting ILM/ISM policy",
				zap.String("name", name),
				zap.Error(err),
			)
			return bytes, err
		}
		return bytes, nil
	}

	_, err := backoff.Retry(
		ctx,
		operation,
		backoff.WithMaxTries(maxTries),
		backoff.WithMaxElapsedTime(maxElapsedTime),
	)

	var respError ResponseError
	if errors.As(err, &respError) {
		if respError.StatusCode == http.StatusNotFound {
			return false, nil
		}
	}
	if err != nil {
		return false, fmt.Errorf("failed to get ILM policy: %s, %w", name, err)
	}
	return true, nil
}
