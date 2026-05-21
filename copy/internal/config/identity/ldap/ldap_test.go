package ldap

import (
	"errors"
	"testing"
)

func TestWrapAuthError(t *testing.T) {
	baseErr := errors.New("LDAP auth failed for DN uid=dillon,dc=min,dc=io")
	err := wrapAuthError(baseErr)

	if !IsAuthError(err) {
		t.Fatal("expected wrapped error to be recognized as auth failure")
	}
	if err.Error() != baseErr.Error() {
		t.Fatalf("expected error text %q, got %q", baseErr.Error(), err.Error())
	}
}

func TestWrapAuthErrorNil(t *testing.T) {
	if wrapAuthError(nil) != nil {
		t.Fatal("expected nil input to stay nil")
	}
}

func TestIsAuthErrorNegative(t *testing.T) {
	if IsAuthError(errors.New("ldap unavailable")) {
		t.Fatal("expected plain errors to not be classified as auth failures")
	}
}

func TestIsUserDNNotFoundError(t *testing.T) {
	if isUserDNNotFoundError(nil) {
		t.Fatal("expected nil error to not be detected as user-not-found")
	}
	if !isUserDNNotFoundError(errors.New("user DN not found for: dillon")) {
		t.Fatal("expected lowercase user-not-found error to be detected")
	}
	if !isUserDNNotFoundError(errors.New("User DN not found for: dillon")) {
		t.Fatal("expected legacy uppercase user-not-found error to be detected")
	}
	if isUserDNNotFoundError(errors.New("base DN (dc=min,dc=io) for user DN search does not exist")) {
		t.Fatal("expected infrastructure lookup error to not be detected as user-not-found")
	}
}

func TestSetSTSTrustedProxies(t *testing.T) {
	var cfg Config
	if err := cfg.SetSTSTrustedProxies("192.0.2.10,198.51.100.0/24;2001:db8::/126"); err != nil {
		t.Fatalf("expected trusted proxies to parse, got %v", err)
	}

	tests := []struct {
		peerIP string
		want   bool
	}{
		{peerIP: "192.0.2.10", want: true},
		{peerIP: "198.51.100.23", want: true},
		{peerIP: "198.51.101.23", want: false},
		{peerIP: "2001:db8::2", want: true},
		{peerIP: "2001:db8::8", want: false},
	}

	for _, tt := range tests {
		if got := cfg.IsSTSTrustedProxy(tt.peerIP); got != tt.want {
			t.Fatalf("peer %q: expected %v, got %v", tt.peerIP, tt.want, got)
		}
	}
}

func TestSetSTSTrustedProxiesRejectsInvalidEntries(t *testing.T) {
	var cfg Config
	if err := cfg.SetSTSTrustedProxies("192.0.2.0/24,not-an-ip"); err == nil {
		t.Fatal("expected invalid trusted proxy list to fail")
	}
}
