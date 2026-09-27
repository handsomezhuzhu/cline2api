package main

import (
	"net/http"
	"net/url"
	"testing"
)

// TestHTTPTransportUsesHTTPSProxyFromEnvironment 验证全局 transport 的 Proxy 钩子
// 走 clineOutboundProxy 链路：未配置应用内代理池时回退环境变量代理解析。
//
// 不直接依赖 t.Setenv + 真实 http.ProxyFromEnvironment：后者进程级一次性
// 缓存环境变量，全量测试下其他用例先触发缓存后本用例的 Setenv 即失效，
// 造成「单跑过、全量挂」的抖动。改为经 clineEnvProxy 接缝注入替身
// （环境变量读取本身是标准库行为，无需重复测试）。
func TestHTTPTransportUsesHTTPSProxyFromEnvironment(t *testing.T) {
	resetClineProxyTestState(t) // 确保应用内代理池未生效，钩子才会回退环境代理

	want, err := url.Parse("http://127.0.0.1:8080")
	if err != nil {
		t.Fatalf("parse expected proxy URL: %v", err)
	}
	oldEnvProxy := clineEnvProxy
	clineEnvProxy = func(*http.Request) (*url.URL, error) { return want, nil }
	t.Cleanup(func() { clineEnvProxy = oldEnvProxy })

	req, err := http.NewRequest(http.MethodPost, "https://api.workos.com/user_management/authorize/device", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	proxyURL, err := httpTransport.Proxy(req)
	if err != nil {
		t.Fatalf("resolve proxy: %v", err)
	}
	if proxyURL == nil {
		t.Fatal("expected proxy hook to delegate to environment proxy")
	}
	if proxyURL.String() != want.String() {
		t.Fatalf("proxy URL = %q, want %q", proxyURL, want)
	}
}
