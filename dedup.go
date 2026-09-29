package main

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// 账号一键去重。
//
// 为什么需要：导入时的去重按 refreshToken 比对，但 WorkOS 会轮换 refresh token ——
// 同一个 Cline 账号二次登录/刷新后拿到新 token 再次导入，池子里就会出现
// 「同一账号多份凭证」。按 token 去重挡不住这种情况，必须以账号真实身份
// （/api/v1/users/me 返回的 uid）为准。
//
// 同一 uid 的多个条目：保留池中第一个，其余删除，并把被删条目的用量/消耗
// 统计合并进保留条目（不丢历史数据）。uid 解析失败（token 已死等）的账号
// 无法判定身份，保持原样并计入 unresolved。

// dedupGroup 是一组重复账号（同一 uid）。
type dedupGroup struct {
	UID    string   `json:"uid"`
	Emails []string `json:"emails"`
	Kept   string   `json:"kept"` // 保留条目的 email
}

// dedupProgress 是去重任务的状态（供前端轮询）。
type dedupProgress struct {
	Running   bool          `json:"running"`
	Total     int           `json:"total"`
	Done      int           `json:"done"`
	Removed   int           `json:"removed"`
	Kept      int           `json:"kept"`
	Unresolved int          `json:"unresolved"`
	Groups    []dedupGroup  `json:"groups,omitempty"`
	StartedAt time.Time     `json:"startedAt,omitempty"`
	FinishedAt time.Time    `json:"finishedAt,omitempty"`
	Error     string        `json:"error,omitempty"`
}

var (
	dedupMu      sync.Mutex
	dedupRunning bool
	dedupLast    dedupProgress
)

// startAccountDedup 后台启动去重任务；已有任务在运行时直接返回当前进度。
func startAccountDedup() (dedupProgress, bool) {
	dedupMu.Lock()
	if dedupRunning {
		p := dedupLast
		dedupMu.Unlock()
		return p, false
	}

	p := loadPool()
	poolMu.Lock()
	targets := make([]*Account, len(p.Accounts))
	copy(targets, p.Accounts)
	poolMu.Unlock()

	dedupRunning = true
	dedupLast = dedupProgress{
		Running:   true,
		Total:     len(targets),
		StartedAt: time.Now(),
	}
	prog := dedupLast
	dedupMu.Unlock()

	go runAccountDedup(targets)
	return prog, true
}

// getAccountDedupProgress 返回去重任务进度（无任务时返回上一次终态）。
func getAccountDedupProgress() dedupProgress {
	dedupMu.Lock()
	defer dedupMu.Unlock()
	return dedupLast
}

// runAccountDedup 并发解析每个账号的 uid，再按 uid 分组去重。
func runAccountDedup(targets []*Account) {
	// 1) 并发解析 uid（优先用 Credits 刷新时缓存的 CreditUserID）
	type resolved struct {
		acc *Account
		uid string
	}
	results := make([]resolved, len(targets))
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		done    int
		failed  int
	)
	sem := make(chan struct{}, creditRefreshConcurrency)
	for i, acc := range targets {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, a *Account) {
			defer wg.Done()
			defer func() { <-sem }()
			uid, err := resolveAccountUID(a)
			mu.Lock()
			results[idx] = resolved{acc: a, uid: uid}
			if err != nil {
				failed++
			}
			done++
			d, f := done, failed
			mu.Unlock()
			dedupMu.Lock()
			dedupLast.Done = d
			dedupLast.Unresolved = f
			dedupMu.Unlock()
			_ = err
		}(i, acc)
	}
	wg.Wait()

	// 2) 按 uid 分组（uid 为空的进 unresolved，不动）
	byUID := make(map[string][]*Account)
	order := make([]string, 0, len(results))
	for _, r := range results {
		if r.uid == "" {
			continue
		}
		if _, ok := byUID[r.uid]; !ok {
			order = append(order, r.uid)
		}
		byUID[r.uid] = append(byUID[r.uid], r.acc)
	}

	// 3) 每组保留第一个，其余删除并合并统计
	var (
		groups []dedupGroup
		removed int
	)
	for _, uid := range order {
		group := byUID[uid]
		if len(group) < 2 {
			continue
		}
		kept := group[0]
		emails := make([]string, 0, len(group))
		for _, a := range group {
			emails = append(emails, a.Email)
		}
		// 合并用量与消耗统计到保留条目
		poolMu.Lock()
		for _, a := range group[1:] {
			kept.UsageCount += a.UsageCount
			kept.PromptTokens += a.PromptTokens
			kept.CompletionTokens += a.CompletionTokens
			kept.TotalTokens += a.TotalTokens
			kept.CachedTokens += a.CachedTokens
			kept.SpentMicroUsd += a.SpentMicroUsd
			kept.SpendCount += a.SpendCount
			if a.LastSpendAt.After(kept.LastSpendAt) {
				kept.LastSpendAt = a.LastSpendAt
			}
		}
		kept.CreditUserID = uid
		poolMu.Unlock()

		for _, a := range group[1:] {
			if removeAccount(a.AccountID) {
				removed++
			}
		}
		groups = append(groups, dedupGroup{UID: uid, Emails: emails, Kept: kept.Email})
	}
	savePool()

	keptCount := 0
	p := loadPool()
	poolMu.Lock()
	keptCount = len(p.Accounts)
	poolMu.Unlock()

	dedupMu.Lock()
	dedupRunning = false
	dedupLast.Running = false
	dedupLast.Removed = removed
	dedupLast.Kept = keptCount
	dedupLast.Groups = groups
	dedupLast.FinishedAt = time.Now()
	dedupMu.Unlock()
	log.Printf("account dedup done: removed %d duplicate(s), %d account(s) kept, %d unresolved", removed, keptCount, failed)
}

// resolveAccountUID 取账号的 Cline uid：优先 Credits 缓存，否则用有效 accessToken
// 调 /api/v1/users/me。失败返回空串（调用方按未解析处理）。
func resolveAccountUID(acc *Account) (string, error) {
	if acc.CreditUserID != "" {
		return acc.CreditUserID, nil
	}
	token, err := ensureAccountToken(acc)
	if err != nil {
		return "", fmt.Errorf("token: %w", err)
	}
	uid, err := fetchClineUserID(token)
	if err != nil {
		return "", err
	}
	poolMu.Lock()
	acc.CreditUserID = uid
	savePoolLocked()
	poolMu.Unlock()
	return uid, nil
}
