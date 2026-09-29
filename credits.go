package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sync"
	"time"
)

// Cline Credits 接口（2026-09-30 实测可用，与 cline2api 现有凭证体系完全复用）：
//
//	GET /api/v1/users/me            -> data.id 是 balance 接口路径参数需要的 uid
//	GET /api/v1/users/{uid}/balance -> data.balance，单位 micro-USD（1e-6 美元）
//
// 两个容易踩的坑：
//   - /api/v1/users/me/balance 路径虽然存在，但 uid 会按 id 格式校验，返回 400，
//     必须先调 /users/me 拿真实 uid。
//   - balance 是 micro-USD；Cline 自己的 UI 按 /1e4 显示成 credits，
//     即 1 credit = $0.01。余额为负表示账号已透支。
//
// 认证方式与代理转发完全一致：Authorization: Bearer workos:<accessToken>，
// accessToken 由账号池内的 refreshToken 经 /api/v1/auth/refresh 得到。

const (
	// clineUsersMeURL /users/me：取当前账号 uid。
	clineUsersMeURL = clineAPIBase + "/users/me"
	// creditRefreshStaleAfter 超过该时间未查询的账号才会被「自动刷新」覆盖。
	creditRefreshStaleAfter = 5 * time.Minute
	// creditRefreshConcurrency 后台刷新 Credits 的并发数（避免打满上游）。
	creditRefreshConcurrency = 4
	// creditRequestTimeout 单次 Credits 查询超时。
	creditRequestTimeout = 15 * time.Second
)

type clineUserMeResp struct {
	Data struct {
		ID    string `json:"id"`
		Email string `json:"email"`
	} `json:"data"`
}

type clineBalanceResp struct {
	Data struct {
		UserID  string   `json:"userId"`
		Balance *float64 `json:"balance"`
	} `json:"data"`
}

// fetchClineUserID 用账号 accessToken 调 /api/v1/users/me 取 uid。
func fetchClineUserID(accessToken string) (string, error) {
	req, err := http.NewRequest(http.MethodGet, clineUsersMeURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: creditRequestTimeout, Transport: httpTransport}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("users/me: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("users/me: status %d", resp.StatusCode)
	}

	var me clineUserMeResp
	if err := json.NewDecoder(resp.Body).Decode(&me); err != nil {
		return "", fmt.Errorf("users/me decode: %w", err)
	}
	if me.Data.ID == "" {
		return "", fmt.Errorf("users/me: empty id")
	}
	return me.Data.ID, nil
}

// fetchClineBalance 调 /api/v1/users/{uid}/balance，返回 micro-USD 余额。
func fetchClineBalance(accessToken, uid string) (float64, error) {
	url := fmt.Sprintf("%s/users/%s/balance", clineAPIBase, uid)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: creditRequestTimeout, Transport: httpTransport}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("balance: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("balance: status %d", resp.StatusCode)
	}

	var b clineBalanceResp
	if err := json.NewDecoder(resp.Body).Decode(&b); err != nil {
		return 0, fmt.Errorf("balance decode: %w", err)
	}
	if b.Data.Balance == nil {
		return 0, fmt.Errorf("balance: missing balance field")
	}
	return *b.Data.Balance, nil
}

// refreshAccountCredit 查询并更新单个账号的 Credits 余额（uid 优先用缓存，
// 没有或失效时重新调 /users/me）。失败时把原因写进 acc.CreditError，
// 保留上一次成功的余额不清空。
func refreshAccountCredit(acc *Account) error {
	token, err := ensureAccountToken(acc)
	if err != nil {
		return setAccountCreditError(acc, err)
	}

	uid := acc.CreditUserID
	if uid == "" {
		uid, err = fetchClineUserID(token)
		if err != nil {
			return setAccountCreditError(acc, err)
		}
	}

	balance, err := fetchClineBalance(token, uid)
	if err != nil {
		// uid 可能已失效（账号被重建等）：清掉缓存下次重取
		poolMu.Lock()
		if acc.CreditUserID != "" {
			acc.CreditUserID = ""
		}
		poolMu.Unlock()
		return setAccountCreditError(acc, err)
	}

	poolMu.Lock()
	acc.CreditUserID = uid
	acc.CreditBalance = &balance
	acc.CreditCheckedAt = time.Now()
	acc.CreditError = ""
	savePoolLocked()
	poolMu.Unlock()
	return nil
}

// setAccountCreditError 记录查询失败原因并落盘（保留旧余额）。
func setAccountCreditError(acc *Account, err error) error {
	poolMu.Lock()
	acc.CreditError = err.Error()
	savePoolLocked()
	poolMu.Unlock()
	return err
}

// creditRefreshProgress 是后台刷新 Credits 任务的状态（供前端轮询）。
type creditRefreshProgress struct {
	Running    bool      `json:"running"`
	Total      int       `json:"total"`
	Done       int       `json:"done"`
	Succeeded  int       `json:"succeeded"`
	Failed     int       `json:"failed"`
	StartedAt  time.Time `json:"startedAt,omitempty"`
	FinishedAt time.Time `json:"finishedAt,omitempty"`
}

var (
	creditRefreshMu      sync.Mutex
	creditRefreshRunning bool
	creditRefreshLast    creditRefreshProgress
)

// startCreditRefresh 后台异步刷新指定账号的 Credits。
// accountIDs 为空表示刷新全部账号；onlyStale=true 时只刷新超过
// creditRefreshStaleAfter 未更新（或从未查询）的账号。已在运行时直接返回。
func startCreditRefresh(accountIDs []string, onlyStale bool) (creditRefreshProgress, bool) {
	creditRefreshMu.Lock()
	if creditRefreshRunning {
		p := creditRefreshLast
		creditRefreshMu.Unlock()
		return p, false
	}

	// 快照目标账号（持 poolMu 拷贝指针，避免长时间持锁）
	p := loadPool()
	poolMu.Lock()
	targets := make([]*Account, 0, len(p.Accounts))
	only := make(map[string]bool, len(accountIDs))
	for _, id := range accountIDs {
		only[id] = true
	}
	for _, a := range p.Accounts {
		if len(only) > 0 && !only[a.AccountID] {
			continue
		}
		if onlyStale && time.Since(a.CreditCheckedAt) < creditRefreshStaleAfter {
			continue
		}
		targets = append(targets, a)
	}
	poolMu.Unlock()

	if len(targets) == 0 {
		p := creditRefreshProgress{Running: false, FinishedAt: time.Now()}
		creditRefreshLast = p
		creditRefreshMu.Unlock()
		return p, true
	}

	creditRefreshRunning = true
	creditRefreshLast = creditRefreshProgress{
		Running:   true,
		Total:     len(targets),
		StartedAt: time.Now(),
	}
	prog := creditRefreshLast
	creditRefreshMu.Unlock()

	go runCreditRefresh(targets)
	return prog, true
}

// runCreditRefresh 以固定并发刷新目标账号，结束后写终态。
func runCreditRefresh(targets []*Account) {
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		succeeded int
		failed    int
	)
	sem := make(chan struct{}, creditRefreshConcurrency)
	for _, acc := range targets {
		wg.Add(1)
		sem <- struct{}{}
		go func(a *Account) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := refreshAccountCredit(a); err != nil {
				log.Printf("credit refresh failed for %s: %v", a.Email, err)
				mu.Lock()
				failed++
				mu.Unlock()
			} else {
				mu.Lock()
				succeeded++
				mu.Unlock()
			}
			mu.Lock()
			done := succeeded + failed
			mu.Unlock()
			creditRefreshMu.Lock()
			creditRefreshLast.Done = done
			creditRefreshLast.Succeeded = succeeded
			creditRefreshLast.Failed = failed
			creditRefreshMu.Unlock()
		}(acc)
	}
	wg.Wait()

	creditRefreshMu.Lock()
	creditRefreshRunning = false
	creditRefreshLast.Running = false
	creditRefreshLast.FinishedAt = time.Now()
	creditRefreshMu.Unlock()
}

// getCreditRefreshProgress 返回后台刷新进度（无任务时返回上一次的终态）。
func getCreditRefreshProgress() creditRefreshProgress {
	creditRefreshMu.Lock()
	defer creditRefreshMu.Unlock()
	return creditRefreshLast
}

// accountCreditInfo 是 /admin/api/accounts/credits 返回的单账号条目。
type accountCreditInfo struct {
	AccountID     string    `json:"accountId"`
	Email         string    `json:"email"`
	CreditBalance *float64  `json:"creditBalance"`
	CreditUserID  string    `json:"creditUserId,omitempty"`
	CreditChecked time.Time `json:"creditCheckedAt,omitempty"`
	CreditError   string    `json:"creditError,omitempty"`
	Fetched       bool      `json:"fetched"`
}

// listAccountCredits 返回全部账号的 Credits 缓存快照（不发起上游请求）。
func listAccountCredits() []accountCreditInfo {
	p := loadPool()
	poolMu.Lock()
	defer poolMu.Unlock()

	out := make([]accountCreditInfo, 0, len(p.Accounts))
	for _, a := range p.Accounts {
		out = append(out, accountCreditInfo{
			AccountID:     a.AccountID,
			Email:         a.Email,
			CreditBalance: a.CreditBalance,
			CreditUserID:  a.CreditUserID,
			CreditChecked: a.CreditCheckedAt,
			CreditError:   a.CreditError,
			Fetched:       !a.CreditCheckedAt.IsZero(),
		})
	}
	return out
}

// needCreditRefresh 判断是否还有账号缺少有效（未过期）的 Credits 缓存，
// 用于前端打开账号页时的按需自动刷新。
func needCreditRefresh() bool {
	p := loadPool()
	poolMu.Lock()
	defer poolMu.Unlock()
	for _, a := range p.Accounts {
		if a.CreditCheckedAt.IsZero() || time.Since(a.CreditCheckedAt) >= creditRefreshStaleAfter {
			return true
		}
	}
	return false
}

// ========== 网关消耗捕获 ==========
//
// Cline 网关的响应（非流式 body / 流式最后一个 chunk）带 usage.cost 字段，
// 单位美元，就是这笔请求计入账号 credits 的消耗：
//
//	"usage": { "prompt_tokens": 86, ..., "cost": 0.0002052,
//	           "market_cost": 0.0002052, "gateway_cost": 0.0002052 }
//
// cost × 1e6 即 micro-USD，与 /users/{uid}/usages 流水的 creditsUsed 一致
// （实测 0.0002052 → 205 micro-USD，响应侧有末位舍入）。免费模型该字段为 0。
// inferenceCost / inputInferenceCost 等是上游推理成本，不收钱时也可能 >0，不能拿来计费。

// costTailBytes 保留响应体尾部的字节数：usage 对象在两种响应形态里都位于末尾，
// 64KB 足以覆盖最长的最后一个 SSE chunk（含 routing 元信息）。
const costTailBytes = 64 * 1024

// costCaptureReader 包装上游响应体：边转发边保留尾部字节，读取结束（EOF 或
// 提前 Close）时解析 usage.cost 并通过 onCost 回调上报（只上报一次）。
type costCaptureReader struct {
	src    io.ReadCloser
	onCost func(costUsd float64)

	tail   []byte // 最近 costTailBytes 字节
	once   sync.Once
	closed bool
}

func newCostCaptureReader(src io.ReadCloser, onCost func(float64)) *costCaptureReader {
	return &costCaptureReader{src: src, onCost: onCost}
}

func (r *costCaptureReader) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	if n > 0 {
		r.tail = append(r.tail, p[:n]...)
		if len(r.tail) > costTailBytes {
			r.tail = r.tail[len(r.tail)-costTailBytes:]
		}
	}
	if err == io.EOF {
		r.flush()
	}
	return n, err
}

func (r *costCaptureReader) Close() error {
	r.flush()
	return r.src.Close()
}

// flush 解析尾部并上报消耗；sync.Once 保证 EOF/Close 只触发一次。
func (r *costCaptureReader) flush() {
	r.once.Do(func() {
		if cost, ok := extractUsageCost(r.tail); ok {
			r.onCost(cost)
		}
	})
}

// extractUsageCost 从响应体尾部解析最后一个 usage 对象里的 cost 字段。
// 兼容非流式（{"data":{...,"usage":{...}}}）与流式（data: {...,"usage":{...}}）。
func extractUsageCost(tail []byte) (float64, bool) {
	idx := bytes.LastIndex(tail, []byte(`"usage"`))
	if idx < 0 {
		return 0, false
	}
	// 定位 "usage" 后的 {
	rel := bytes.IndexByte(tail[idx:], '{')
	if rel < 0 {
		return 0, false
	}
	start := idx + rel
	// 花括号配平找到 usage 对象结尾（跳过字符串字面量内的括号）
	depth, inStr, esc, end := 0, false, false, -1
	for j := start; j < len(tail); j++ {
		c := tail[j]
		if inStr {
			switch {
			case esc:
				esc = false
			case c == '\\':
				esc = true
			case c == '"':
				inStr = false
			}
			continue
		}
		switch c {
		case '"':
			inStr = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				end = j + 1
			}
		}
		if end > 0 {
			break
		}
	}
	if end < 0 {
		return 0, false
	}
	var u struct {
		Cost *float64 `json:"cost"`
	}
	if err := json.Unmarshal(tail[start:end], &u); err != nil || u.Cost == nil {
		return 0, false
	}
	return *u.Cost, true
}

// recordAccountSpend 把一笔请求的消耗（美元）累计到账号上并落盘。
// cost<=0（免费模型/解析不到）不计。
func recordAccountSpend(acc *Account, costUsd float64) {
	if acc == nil || costUsd <= 0 {
		return
	}
	micro := int64(math.Round(costUsd * 1e6))
	if micro <= 0 {
		return
	}
	poolMu.Lock()
	acc.SpentMicroUsd += micro
	acc.SpendCount++
	acc.LastSpendAt = time.Now()
	savePoolLocked()
	poolMu.Unlock()
}
