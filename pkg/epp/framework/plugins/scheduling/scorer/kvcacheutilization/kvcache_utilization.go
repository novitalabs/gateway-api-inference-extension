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
	"encoding/json"
	"math"
	"sync"
	"time"

	fwkdl "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/datalayer"
	fwkplugin "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/plugin"
	fwkrc "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/requestcontrol"
	framework "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/extractor/metrics"
)

const (
	KvCacheUtilizationScorerType = "kv-cache-utilization-scorer"
)

// compile-time type assertion
var _ framework.Scorer = &KVCacheUtilizationScorer{}
var _ fwkrc.PreRequest = &KVCacheUtilizationScorer{}

// KvCacheUtilizationScorerFactory defines the factory function for KVCacheUtilizationScorer.
func KvCacheUtilizationScorerFactory(name string, _ json.RawMessage, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	return NewKVCacheUtilizationScorer().WithName(name), nil
}

// NewKVCacheUtilizationScorer initializes a new KVCacheUtilizationScorer and returns its pointer.
func NewKVCacheUtilizationScorer() *KVCacheUtilizationScorer {
	return &KVCacheUtilizationScorer{
		typedName: fwkplugin.TypedName{Type: KvCacheUtilizationScorerType, Name: KvCacheUtilizationScorerType},
		endpoints: make(map[string]*endpointRankDeltas),
	}
}

// KVCacheUtilizationScorer scores list of candidate endpoints based on KV cache utilization.
type KVCacheUtilizationScorer struct {
	typedName fwkplugin.TypedName
	mutex     sync.Mutex
	endpoints map[string]*endpointRankDeltas
}

type endpointRankDeltas struct {
	metricsUpdateTime time.Time
	rankDeltas        map[string]float64
}

// TypedName returns the type and name tuple of this plugin instance.
func (s *KVCacheUtilizationScorer) TypedName() fwkplugin.TypedName {
	return s.typedName
}

// Category returns the preference the scorer applies when scoring candidate endpoints.
func (s *KVCacheUtilizationScorer) Category() framework.ScorerCategory {
	return framework.Distribution
}

// Consumes returns the list of data that is consumed by the plugin.
func (s *KVCacheUtilizationScorer) Consumes() map[string]any {
	return map[string]any{
		metrics.KVCacheUsagePercentKey:     float64(0),
		metrics.RankKVCacheUsagePercentKey: map[string]float64{},
	}
}

// WithName sets the name of the scorer.
func (s *KVCacheUtilizationScorer) WithName(name string) *KVCacheUtilizationScorer {
	s.typedName.Name = name
	return s
}

// Score returns the scoring result for the given list of endpoints based on context.
func (s *KVCacheUtilizationScorer) Score(_ context.Context, _ *framework.CycleState, _ *framework.InferenceRequest, endpoints []framework.Endpoint) map[framework.Endpoint]float64 {
	scores := make(map[framework.Endpoint]float64, len(endpoints))
	for _, endpoint := range endpoints {
		scores[endpoint] = 1 - s.effectiveMinKVCacheUsage(endpoint)
	}
	return scores
}

func (s *KVCacheUtilizationScorer) PreRequest(_ context.Context, request *framework.InferenceRequest, schedulingResult *framework.SchedulingResult) {
	if schedulingResult == nil || schedulingResult.PrimaryProfileName == "" {
		return
	}
	profileResult := schedulingResult.ProfileResults[schedulingResult.PrimaryProfileName]
	if profileResult == nil {
		return
	}
	for _, endpoint := range profileResult.TargetEndpoints {
		s.incrementSelectedRank(endpoint, request)
	}
}

func (s *KVCacheUtilizationScorer) effectiveMinKVCacheUsage(endpoint framework.Endpoint) float64 {
	endpointName, m, ok := endpointInfo(endpoint)
	if !ok {
		return 0
	}
	if len(m.RankKVCacheUsagePercent) == 0 {
		return clampUsage(m.KVCacheUsagePercent)
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	state := s.stateForUpdateLocked(endpointName, m.UpdateTime)
	minUsage := math.Inf(1)
	for rank, usage := range m.RankKVCacheUsagePercent {
		effective := usage + state.rankDeltas[rank]
		if effective < minUsage {
			minUsage = effective
		}
	}
	if math.IsInf(minUsage, 1) {
		return clampUsage(m.KVCacheUsagePercent)
	}
	return clampUsage(minUsage)
}

func (s *KVCacheUtilizationScorer) incrementSelectedRank(endpoint framework.Endpoint, request *framework.InferenceRequest) {
	endpointName, m, ok := endpointInfo(endpoint)
	if !ok || len(m.RankKVCacheUsagePercent) == 0 {
		return
	}
	delta := estimateKVCacheUsageDelta(request, m)
	if delta <= 0 {
		return
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	state := s.stateForUpdateLocked(endpointName, m.UpdateTime)
	rank := minEffectiveRank(m.RankKVCacheUsagePercent, state.rankDeltas)
	if rank == "" {
		return
	}
	state.rankDeltas[rank] += delta
}

func endpointInfo(endpoint framework.Endpoint) (string, *fwkdl.Metrics, bool) {
	if endpoint == nil || endpoint.GetMetadata() == nil || endpoint.GetMetrics() == nil {
		return "", nil, false
	}
	return endpoint.GetMetadata().NamespacedName.String(), endpoint.GetMetrics(), true
}

func (s *KVCacheUtilizationScorer) stateForUpdateLocked(endpointName string, metricsUpdateTime time.Time) *endpointRankDeltas {
	state, exists := s.endpoints[endpointName]
	if !exists {
		state = &endpointRankDeltas{rankDeltas: make(map[string]float64)}
		s.endpoints[endpointName] = state
	}
	if metricsUpdateTime.After(state.metricsUpdateTime) {
		state.metricsUpdateTime = metricsUpdateTime
		clear(state.rankDeltas)
	}
	return state
}

func minEffectiveRank(rankUsage map[string]float64, rankDeltas map[string]float64) string {
	selected := ""
	minUsage := math.Inf(1)
	for rank, usage := range rankUsage {
		effective := usage + rankDeltas[rank]
		if effective < minUsage {
			selected = rank
			minUsage = effective
		}
	}
	return selected
}

func estimateKVCacheUsageDelta(request *framework.InferenceRequest, m *fwkdl.Metrics) float64 {
	capacity := m.CacheNumBlocks * m.CacheBlockSize
	if capacity <= 0 {
		capacity = m.KvCacheMaxTokenCapacity
	}
	if capacity <= 0 {
		return 0
	}
	tokens := estimateInputTokens(request)
	if tokens <= 0 {
		return 0
	}
	return float64(tokens) / float64(capacity)
}

func estimateInputTokens(request *framework.InferenceRequest) int {
	if request == nil {
		return 0
	}
	if request.Body != nil {
		if hint := request.Body.InputTokenCountHint(); hint > 0 {
			return hint
		}
		if tokenized := request.Body.TokenizedPrompt; tokenized != nil && len(tokenized.TokenIDs) > 0 {
			return len(tokenized.TokenIDs)
		}
		if prompt := request.Body.PromptText(); prompt != "" {
			tokens := len(prompt) / 4
			if tokens == 0 {
				return 1
			}
			return tokens
		}
	}
	if request.RequestSizeBytes > 0 {
		tokens := request.RequestSizeBytes / 4
		if tokens == 0 {
			return 1
		}
		return tokens
	}
	return 0
}

func clampUsage(usage float64) float64 {
	switch {
	case usage < 0:
		return 0
	case usage > 1:
		return 1
	default:
		return usage
	}
}
