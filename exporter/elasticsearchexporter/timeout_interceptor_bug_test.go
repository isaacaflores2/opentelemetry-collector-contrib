// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

// TestTimeoutInterceptor_CancelsResponseBodyRead demonstrates the bug introduced by
// https://github.com/open-telemetry/opentelemetry-collector-contrib/pull/50329.
//
// timeoutInterceptor (esclient.go) wraps every request with:
//
//	ctx, cancel := context.WithTimeout(req.Context(), perRequestTimeout)
//	defer cancel()
//	return next(req.WithContext(ctx))
//
// In elastictransport, the interceptor's `next` is the underlying http.RoundTripper
// (Client.roundTrip wraps c.transport.RoundTrip with the interceptor):
//
//	https://github.com/elastic/elastic-transport-go/blob/v8.11.0/elastictransport/elastictransport.go#L557-L561
//
// An http.RoundTripper returns as soon as the response HEADERS are read; the response
// body is still an unread network stream at that point. Client.Perform runs the round
// trip and returns that response WITHOUT reading/buffering the body on the success path
// (the body is only drained on the retry path):
//
//	https://github.com/elastic/elastic-transport-go/blob/v8.11.0/elastictransport/elastictransport.go#L445  (res, err = c.roundTrip(req))
//	https://github.com/elastic/elastic-transport-go/blob/v8.11.0/elastictransport/elastictransport.go#L502  (io.Copy(io.Discard, res.Body) -- retry path only)
//	https://github.com/elastic/elastic-transport-go/blob/v8.11.0/elastictransport/elastictransport.go#L530  (return res, err -- body still unread)
//
// The deferred cancel() therefore fires immediately after headers are received,
// cancelling the request context BEFORE go-docappender reads and decodes res.Body.
//
// The body read then races connection teardown and fails with "context canceled" /
// EOF, even though Elasticsearch returned a healthy 2xx with a complete body far within
// the configured timeout. This reproduces the observed production symptom: the proxy/ES
// logged complete 200 _bulk responses, yet the collector logged
// `error decoding bulk response: EOF` / `... context canceled`.
//
// The two subtests isolate the defect: the only difference between them is whether the
// body is written together with the headers (succeeds) or shortly after the headers are
// flushed (fails). Both complete well within the 30s client timeout, so the failure is
// premature cancellation, not a genuine timeout.
func TestTimeoutInterceptor_CancelsResponseBodyRead(t *testing.T) {
	const (
		// Deliberately huge relative to how long the server takes to respond, so a
		// correct timeout implementation MUST succeed. Any failure is a premature cancel.
		clientTimeout = 30 * time.Second
		// Delay between flushing response headers and writing the body. Models
		// Elasticsearch streaming the body slightly after committing 200 headers.
		bodyDelay = 200 * time.Millisecond
	)

	// A valid single-item bulk success response (one log record -> one item).
	const bulkResponseBody = `{"took":1,"errors":false,"items":[{"create":{"_index":"logs-generic-default","status":201}}]}`

	newServer := func(t *testing.T, delayBodyAfterHeaders bool) *httptest.Server {
		return newESTestServerBulkHandlerFunc(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if delayBodyAfterHeaders {
				// Commit and flush the 200 headers so the client's RoundTrip
				// returns (and #50329's deferred cancel fires) before the body
				// is written.
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				time.Sleep(bodyDelay)
			}
			_, _ = w.Write([]byte(bulkResponseBody))
		})
	}

	send := func(t *testing.T, server *httptest.Server) (error, time.Duration) {
		exp := newTestLogsExporter(t, server.URL, func(cfg *Config) {
			cfg.ClientConfig.Timeout = clientTimeout
			// Disable retries so the first flush outcome surfaces directly and the
			// proof is deterministic.
			cfg.Retry.Enabled = false
			// Flush immediately and block until the result is known.
			cfg.QueueBatchConfig.Get().Batch.Get().MinSize = 0
			cfg.QueueBatchConfig.Get().WaitForResult = true
		})

		logs := plog.NewLogs()
		logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		logs.MarkReadOnly()

		start := time.Now()
		err := exp.ConsumeLogs(t.Context(), logs)
		return err, time.Since(start)
	}

	// Control: headers + body written together. The exporter must index the record
	// successfully, proving the response body and the test harness are valid.
	t.Run("body_flushed_with_headers_succeeds", func(t *testing.T) {
		err, elapsed := send(t, newServer(t, false))
		require.NoError(t, err, "a healthy bulk response should be indexed successfully")
		assert.Less(t, elapsed, clientTimeout)
	})

	// Bug: identical healthy response, but the body arrives shortly after the headers
	// are flushed - still far within the 30s timeout. #50329's deferred cancel cancels
	// the request context right after headers, so go-docappender's body read/decode fails.
	t.Run("body_delayed_after_headers_fails_due_to_50329", func(t *testing.T) {
		err, elapsed := send(t, newServer(t, true))

		require.Error(t, err,
			"BUG(#50329): the response body read is cancelled after headers, "+
				"despite a healthy 2xx response delivered within the timeout")

		errMsg := err.Error()
		assert.True(t,
			errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) ||
				strings.Contains(errMsg, "context canceled") ||
				strings.Contains(errMsg, "EOF"),
			"expected a context-cancellation / EOF from the premature body-read cancel, got: %v", err)

		// Proof it is NOT a genuine timeout: the failure is effectively immediate,
		// far below the 30s client timeout.
		assert.Less(t, elapsed, clientTimeout/2,
			"failure should be immediate (premature cancel), not a real timeout")

		t.Logf("observed exporter error (production symptom): %v", err)
	})
}
