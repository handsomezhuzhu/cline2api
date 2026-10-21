package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// vmc 模型非流式适配（方案 B：只对 vmc/* 前缀生效）。
//
// 背景：Cline 网关对 vmc/* 隐藏模型的非流式（stream=false）请求是坏的 ——
// 实测稳定返回 500 "failed to invoke model 'invalid' from Vercel: Model
// 'invalid' not found"，而同一请求 stream=true 全部 200。直接透传非流式
// 会被代理的降级链当成失败，静默改服务 free 链第一个模型（如
// cline-free/minimax-m3.1-flash），客户端拿到的根本不是点名的模型。
//
// 做法：非流式请求内部改走流式打上游，把 SSE 聚合成与非流式响应同构的
// 结果，再按原协议（OpenAI / Anthropic / Responses）返回。流式请求零影响。

// isVmcModel 判定是否为 vmc 隐藏模型（推荐接口不下发、需内部转流式的批次）。
func isVmcModel(model string) bool {
	return strings.HasPrefix(model, "vmc/")
}

// sseToolCallAcc 累积一个 tool_call 的各分片（按 index 归并）。
type sseToolCallAcc struct {
	id        string
	name      string
	arguments strings.Builder
}

// sseAggregator 把上游 chat SSE 聚合成非流式响应字段。
type sseAggregator struct {
	id           string
	object       string
	created      any
	model        string
	content      strings.Builder
	reasoning    strings.Builder
	finishReason any
	toolCalls    map[int]*sseToolCallAcc
	toolOrder    []int
	usage        map[string]any
	chunks       int
}

func newSSEAggregator() *sseAggregator {
	return &sseAggregator{toolCalls: map[int]*sseToolCallAcc{}}
}

// absorb 吸收一个 chat.completion.chunk（已剥掉 data 信封）。
// 返回 chunk 内的 in-band 错误（Cline 网关会用 200 + error 事件报错）。
func (a *sseAggregator) absorb(obj map[string]any) error {
	if errPayload, ok := obj["error"]; ok {
		if em, ok := errPayload.(map[string]any); ok {
			if msg, ok := em["message"].(string); ok && msg != "" {
				return fmt.Errorf("%s", msg)
			}
		}
		return fmt.Errorf("upstream stream error")
	}
	a.chunks++
	if a.id == "" {
		if v, ok := obj["id"].(string); ok {
			a.id = v
		}
	}
	if a.object == "" {
		if v, ok := obj["object"].(string); ok {
			a.object = v
		}
	}
	if a.created == nil {
		a.created = obj["created"]
	}
	if a.model == "" {
		if v, ok := obj["model"].(string); ok {
			a.model = v
		}
	}
	if u, ok := obj["usage"].(map[string]any); ok {
		a.usage = u // 最后一个带 usage 的 chunk 胜出
	}

	choices, _ := obj["choices"].([]any)
	for _, ch := range choices {
		c, ok := ch.(map[string]any)
		if !ok {
			continue
		}
		if fr := c["finish_reason"]; fr != nil {
			a.finishReason = fr
		}
		delta, ok := c["delta"].(map[string]any)
		if !ok {
			// 个别实现在末包用 message 而非 delta
			delta, ok = c["message"].(map[string]any)
			if !ok {
				continue
			}
		}
		if s, ok := delta["content"].(string); ok {
			a.content.WriteString(s)
		}
		if s, ok := delta["reasoning"].(string); ok {
			a.reasoning.WriteString(s)
		}
		if s, ok := delta["reasoning_content"].(string); ok {
			a.reasoning.WriteString(s)
		}
		tcs, _ := delta["tool_calls"].([]any)
		for _, raw := range tcs {
			tc, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			idx := 0
			if v, ok := tc["index"].(float64); ok {
				idx = int(v)
			}
			entry := a.toolCalls[idx]
			if entry == nil {
				entry = &sseToolCallAcc{}
				a.toolCalls[idx] = entry
				a.toolOrder = append(a.toolOrder, idx)
			}
			if v, ok := tc["id"].(string); ok && v != "" && entry.id == "" {
				entry.id = v
			}
			if fn, ok := tc["function"].(map[string]any); ok {
				if v, ok := fn["name"].(string); ok && v != "" && entry.name == "" {
					entry.name = v
				}
				if v, ok := fn["arguments"].(string); ok {
					entry.arguments.WriteString(v)
				}
			}
		}
	}
	return nil
}

// build 产出与上游非流式响应同构的 map（未经 normalizeOpenAIResponse）。
func (a *sseAggregator) build() map[string]any {
	message := map[string]any{"role": "assistant", "content": a.content.String()}
	if a.reasoning.Len() > 0 {
		message["reasoning_content"] = a.reasoning.String()
	}
	if len(a.toolOrder) > 0 {
		tcs := make([]any, 0, len(a.toolOrder))
		for _, idx := range a.toolOrder {
			e := a.toolCalls[idx]
			tcs = append(tcs, map[string]any{
				"index": idx,
				"id":    e.id,
				"type":  "function",
				"function": map[string]any{
					"name":      e.name,
					"arguments": e.arguments.String(),
				},
			})
		}
		message["tool_calls"] = tcs
	}
	out := map[string]any{
		"id":      a.id,
		"object":  "chat.completion",
		"created": a.created,
		"model":   a.model,
		"choices": []any{map[string]any{
			"index":         0,
			"message":       message,
			"finish_reason": a.finishReason,
		}},
	}
	if a.usage != nil {
		out["usage"] = a.usage
	}
	return out
}

// aggregateSSEToCompletion 读取上游 SSE 流，聚合成非流式响应 map 与 token 用量。
// 一个数据 chunk 都没收到、或流内出现 in-band 错误事件时返回错误
// （后者是 Cline 网关用 200 + error 事件上报失败的方式，不能当成功吞掉）。
func aggregateSSEToCompletion(body io.Reader) (map[string]any, tokenUsage, error) {
	agg := newSSEAggregator()
	var usage tokenUsage

	reader := bufio.NewReader(body)
	for {
		line, err := reader.ReadString('\n')
		if trimmed := strings.TrimRight(line, "\r\n"); strings.HasPrefix(trimmed, "data:") {
			payload := strings.TrimSpace(trimmed[5:])
			if payload != "" && payload != "[DONE]" {
				var obj map[string]any
				if json.Unmarshal([]byte(payload), &obj) == nil {
					obj = unwrapDataEnvelope(obj)
					if absorbErr := agg.absorb(obj); absorbErr != nil {
						return nil, tokenUsage{}, absorbErr
					}
					if u := parseTokenUsage(obj["usage"]); u.Valid {
						usage = mergeTokenUsage(usage, u)
					}
				}
			}
		}
		if err != nil {
			if err != io.EOF {
				return nil, tokenUsage{}, err
			}
			break
		}
	}

	if agg.chunks == 0 {
		return nil, tokenUsage{}, fmt.Errorf("upstream returned empty stream")
	}
	return agg.build(), usage, nil
}

// handleNonStreamViaStream 内部以流式请求上游，聚合为 NonStream 响应返回。
// 用于绕过 Cline 网关 vmc 模型非流式路径的 500（model 'invalid'）。
func handleNonStreamViaStream(w http.ResponseWriter, upstream *http.Response, acc *Account, reqLog *RequestLog) {
	out, _, err := aggregateSSEToCompletion(upstream.Body)
	if err != nil {
		finalizeRequestLog(reqLog, tokenUsage{}, time.Time{}, reqLog.StartedAt, false, "aggregate stream: "+err.Error())
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error": map[string]string{"message": err.Error(), "type": "api_error"},
		})
		return
	}
	completeNonStream(w, out, acc, reqLog, upstream)
}
