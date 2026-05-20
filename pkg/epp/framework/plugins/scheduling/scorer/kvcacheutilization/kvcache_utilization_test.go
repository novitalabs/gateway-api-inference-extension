/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kvcacheutilization

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/datalayer"
	fwkrh "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/requesthandling"
	fwksched "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
)

func newKVEndpoint(name string, metrics *fwkdl.Metrics) fwksched.Endpoint {
	return fwksched.NewEndpoint(
		&fwkdl.EndpointMetadata{
			NamespacedName: k8stypes.NamespacedName{Name: name, Namespace: "default"},
		},
		metrics,
		nil,
	)
}

func newSchedulingResult(primaryProfile string, endpoint fwksched.Endpoint) *fwksched.SchedulingResult {
	return &fwksched.SchedulingResult{
		PrimaryProfileName: primaryProfile,
		ProfileResults: map[string]*fwksched.ProfileRunResult{
			primaryProfile: {TargetEndpoints: []fwksched.Endpoint{endpoint}},
		},
	}
}

func TestKvCacheUtilizationScorer(t *testing.T) {
	tests := []struct {
		name                   string
		endpoints              []fwksched.Endpoint
		expectedScoresEndpoint map[int]float64 // Map of endpoint index to expected score
	}{
		{
			name: "Different KV cache utilization",
			endpoints: []fwksched.Endpoint{
				fwksched.NewEndpoint(&fwkdl.EndpointMetadata{}, &fwkdl.Metrics{KVCacheUsagePercent: 0.8}, nil),
				fwksched.NewEndpoint(&fwkdl.EndpointMetadata{}, &fwkdl.Metrics{KVCacheUsagePercent: 0.5}, nil),
				fwksched.NewEndpoint(&fwkdl.EndpointMetadata{}, &fwkdl.Metrics{KVCacheUsagePercent: 0.0}, nil),
			},
			expectedScoresEndpoint: map[int]float64{
				0: 0.2, // Highest KV cache usage (0.8) gets lowest score (1-0.8=0.2)
				1: 0.5, // Medium KV cache usage (0.5) gets medium score (1-0.5=0.5)
				2: 1.0, // No KV cache usage (0.0) gets highest score (1-0=1.0)
			},
		},
		{
			name: "Same KV cache utilization",
			endpoints: []fwksched.Endpoint{
				fwksched.NewEndpoint(&fwkdl.EndpointMetadata{}, &fwkdl.Metrics{KVCacheUsagePercent: 0.6}, nil),
				fwksched.NewEndpoint(&fwkdl.EndpointMetadata{}, &fwkdl.Metrics{KVCacheUsagePercent: 0.6}, nil),
			},
			expectedScoresEndpoint: map[int]float64{
				0: 0.4, // Both get same score (1-0.6=0.4)
				1: 0.4,
			},
		},
		{
			name: "Zero KV cache utilization",
			endpoints: []fwksched.Endpoint{
				fwksched.NewEndpoint(&fwkdl.EndpointMetadata{}, &fwkdl.Metrics{KVCacheUsagePercent: 0.0}, nil),
				fwksched.NewEndpoint(&fwkdl.EndpointMetadata{}, &fwkdl.Metrics{KVCacheUsagePercent: 0.0}, nil),
			},
			expectedScoresEndpoint: map[int]float64{
				0: 1.0, // No KV cache usage gets highest score
				1: 1.0,
			},
		},
		{
			name: "Full KV cache utilization",
			endpoints: []fwksched.Endpoint{
				fwksched.NewEndpoint(&fwkdl.EndpointMetadata{}, &fwkdl.Metrics{KVCacheUsagePercent: 1.0}, nil),
				fwksched.NewEndpoint(&fwkdl.EndpointMetadata{}, &fwkdl.Metrics{KVCacheUsagePercent: 0.5}, nil),
			},
			expectedScoresEndpoint: map[int]float64{
				0: 0.0, // Full KV cache (1.0) gets lowest score (1-1=0)
				1: 0.5, // Half KV cache (0.5) gets medium score (1-0.5=0.5)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scores := NewKVCacheUtilizationScorer().Score(context.Background(), fwksched.NewCycleState(), &fwksched.InferenceRequest{}, test.endpoints)

			for i, endpoint := range test.endpoints {
				expectedScore := test.expectedScoresEndpoint[i]
				assert.InDelta(t, expectedScore, scores[endpoint], 0.0001, "Endpoint %d should have score %f", i, expectedScore)
			}
		})
	}
}

func TestKvCacheUtilizationScorer_UsesMinimumRankUsage(t *testing.T) {
	endpoints := []fwksched.Endpoint{
		newKVEndpoint("a", &fwkdl.Metrics{
			KVCacheUsagePercent: 0.95,
			RankKVCacheUsagePercent: map[string]float64{
				"engine=0": 0.9,
				"engine=1": 0.2,
			},
		}),
		newKVEndpoint("b", &fwkdl.Metrics{
			KVCacheUsagePercent: 0.1,
			RankKVCacheUsagePercent: map[string]float64{
				"engine=0": 0.4,
				"engine=1": 0.5,
			},
		}),
	}

	scores := NewKVCacheUtilizationScorer().Score(context.Background(), fwksched.NewCycleState(), nil, endpoints)

	assert.InDelta(t, 0.8, scores[endpoints[0]], 0.0001)
	assert.InDelta(t, 0.6, scores[endpoints[1]], 0.0001)
}

func TestKvCacheUtilizationScorer_PreRequestUpdatesMinimumRank(t *testing.T) {
	scorer := NewKVCacheUtilizationScorer()
	now := time.Now()
	endpoint := newKVEndpoint("a", &fwkdl.Metrics{
		RankKVCacheUsagePercent: map[string]float64{
			"engine=0": 0.1,
			"engine=1": 0.5,
		},
		CacheNumBlocks: 100,
		CacheBlockSize: 10,
		UpdateTime:     now,
	})
	request := &fwksched.InferenceRequest{
		Body: &fwkrh.InferenceRequestBody{
			TokenizedPrompt: &fwkrh.TokenizedPrompt{TokenIDs: make([]uint32, 100)},
		},
	}

	before := scorer.Score(context.Background(), fwksched.NewCycleState(), request, []fwksched.Endpoint{endpoint})
	require.InDelta(t, 0.9, before[endpoint], 0.0001)

	scorer.PreRequest(context.Background(), request, newSchedulingResult("decode", endpoint))

	after := scorer.Score(context.Background(), fwksched.NewCycleState(), request, []fwksched.Endpoint{endpoint})
	assert.InDelta(t, 0.8, after[endpoint], 0.0001)
}

func TestKvCacheUtilizationScorer_MetricsUpdateResetsLocalRankDeltas(t *testing.T) {
	scorer := NewKVCacheUtilizationScorer()
	t0 := time.Now()
	endpointT0 := newKVEndpoint("a", &fwkdl.Metrics{
		RankKVCacheUsagePercent: map[string]float64{"engine=0": 0.1},
		CacheNumBlocks:          100,
		CacheBlockSize:          10,
		UpdateTime:              t0,
	})
	request := &fwksched.InferenceRequest{RequestSizeBytes: 400}

	scorer.PreRequest(context.Background(), request, newSchedulingResult("decode", endpointT0))
	afterDelta := scorer.Score(context.Background(), fwksched.NewCycleState(), request, []fwksched.Endpoint{endpointT0})
	require.InDelta(t, 0.8, afterDelta[endpointT0], 0.0001)

	endpointT1 := newKVEndpoint("a", &fwkdl.Metrics{
		RankKVCacheUsagePercent: map[string]float64{"engine=0": 0.1},
		CacheNumBlocks:          100,
		CacheBlockSize:          10,
		UpdateTime:              t0.Add(time.Second),
	})
	afterUpdate := scorer.Score(context.Background(), fwksched.NewCycleState(), request, []fwksched.Endpoint{endpointT1})
	assert.InDelta(t, 0.9, afterUpdate[endpointT1], 0.0001)
}
