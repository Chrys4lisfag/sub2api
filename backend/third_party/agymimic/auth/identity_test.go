package auth

import (
	"regexp"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestDeriveIdentity_Deterministic(t *testing.T) {
	id1 := DeriveIdentity("email:foo@gmail.com")
	id2 := DeriveIdentity("email:foo@gmail.com")
	if id1 != id2 {
		t.Errorf("DeriveIdentity not deterministic:\n  1=%+v\n  2=%+v", id1, id2)
	}
}

func TestDeriveIdentity_DifferentAccountsDifferent(t *testing.T) {
	a := DeriveIdentity("email:alice@gmail.com")
	b := DeriveIdentity("email:bob@gmail.com")
	if a.InstallationID == b.InstallationID {
		t.Errorf("same InstallationID for alice and bob: %s", a.InstallationID)
	}
	if a.ConnectionID == b.ConnectionID {
		t.Errorf("same ConnectionID for alice and bob: %s", a.ConnectionID)
	}
	if a.InstanceLabel == b.InstanceLabel {
		t.Errorf("same InstanceLabel for alice and bob: %s", a.InstanceLabel)
	}
}

func TestDeriveIdentity_NamespaceSeparation(t *testing.T) {
	// Same seed must NOT collide across installation/connection (different salts).
	id := DeriveIdentity("email:x@gmail.com")
	if id.InstallationID == id.ConnectionID {
		t.Errorf("salts didn't separate: install == connection: %s", id.InstallationID)
	}
}

func TestDeriveIdentity_UUIDShape(t *testing.T) {
	id := DeriveIdentity("email:x@gmail.com")
	for _, s := range []string{id.InstallationID, id.ConnectionID} {
		u, err := uuid.Parse(s)
		if err != nil {
			t.Errorf("not a valid UUID: %q (%v)", s, err)
			continue
		}
		if u.Version() != 4 {
			t.Errorf("UUID %s has version %d, want 4", s, u.Version())
		}
		if u.Variant() != uuid.RFC4122 {
			t.Errorf("UUID %s variant != RFC4122", s)
		}
	}
}

func TestDeriveIdentity_InstanceLabelShape(t *testing.T) {
	re := regexp.MustCompile(`^DESKTOP-[A-Z0-9]{7}\\[a-z]+-DESKTOP-[A-Z0-9]{7}$`)
	for _, seed := range []string{"email:a@b.com", "email:long.user.name+tag@gmail.com", "rt:abcdef0123456789"} {
		id := DeriveIdentity(seed)
		if !re.MatchString(id.InstanceLabel) {
			t.Errorf("InstanceLabel %q doesn't match Windows-host pattern", id.InstanceLabel)
		}
		// Both host instances must be identical (HOST\user-HOST).
		parts := strings.Split(id.InstanceLabel, "\\")
		if len(parts) != 2 {
			t.Fatalf("InstanceLabel split wrong: %q", id.InstanceLabel)
		}
		host := parts[0]
		userHost := parts[1]
		if !strings.HasSuffix(userHost, "-"+host) {
			t.Errorf("InstanceLabel host mismatch: %q", id.InstanceLabel)
		}
	}
}

func TestDeriveIdentity_EmptySeedFallsBackToRandom(t *testing.T) {
	id1 := DeriveIdentity("")
	id2 := DeriveIdentity("")
	if id1.InstallationID == id2.InstallationID && id1.ConnectionID == id2.ConnectionID {
		t.Errorf("empty seed should fall back to random; got identical IDs")
	}
}

func TestAccountSeed_EmailPriority(t *testing.T) {
	t1 := &Tokens{Email: "Foo@Example.com  ", RefreshToken: "rt-x"}
	t2 := &Tokens{Email: "foo@example.com", RefreshToken: "rt-y"}
	s1 := AccountSeed(t1)
	s2 := AccountSeed(t2)
	if s1 != s2 {
		t.Errorf("email should be lowercased+trimmed, but seeds differ: %q vs %q", s1, s2)
	}
	if !strings.HasPrefix(s1, "email:") {
		t.Errorf("seed should be email-prefixed: %q", s1)
	}
}

func TestAccountSeed_RefreshTokenFallback(t *testing.T) {
	tok := &Tokens{RefreshToken: "rt-xyz"}
	s := AccountSeed(tok)
	if !strings.HasPrefix(s, "rt:") {
		t.Errorf("seed should fall back to rt:<hash>, got %q", s)
	}
	if strings.Contains(s, "rt-xyz") {
		t.Errorf("seed leaks refresh_token verbatim: %q", s)
	}
}

func TestEnsureIdentity_FillsEmptyFields(t *testing.T) {
	tok := &Tokens{Email: "e2e@gmail.com"}
	changed := EnsureIdentity(tok)
	if !changed {
		t.Fatal("EnsureIdentity returned false but should have filled")
	}
	if tok.InstallationID == "" || tok.ConnectionID == "" || tok.InstanceLabel == "" {
		t.Errorf("EnsureIdentity didn't fill: %+v", tok)
	}
}

func TestEnsureIdentity_IdempotentOnRefilled(t *testing.T) {
	tok := &Tokens{Email: "e2e@gmail.com"}
	EnsureIdentity(tok)
	first := *tok
	if changed := EnsureIdentity(tok); changed {
		t.Error("EnsureIdentity should be no-op on already-filled Tokens")
	}
	if tok.InstallationID != first.InstallationID ||
		tok.ConnectionID != first.ConnectionID ||
		tok.InstanceLabel != first.InstanceLabel {
		t.Error("second call mutated identity fields")
	}
}

func TestEnsureIdentity_RelogProducesSameIdentity(t *testing.T) {
	// Simulate user logging in twice — different refresh_token, same email.
	t1 := &Tokens{Email: "user@gmail.com", RefreshToken: "first-rt"}
	t2 := &Tokens{Email: "user@gmail.com", RefreshToken: "second-rt"}
	EnsureIdentity(t1)
	EnsureIdentity(t2)
	if t1.InstallationID != t2.InstallationID {
		t.Errorf("InstallationID differs after re-login:\n  t1=%s\n  t2=%s", t1.InstallationID, t2.InstallationID)
	}
	if t1.ConnectionID != t2.ConnectionID {
		t.Errorf("ConnectionID differs after re-login")
	}
	if t1.InstanceLabel != t2.InstanceLabel {
		t.Errorf("InstanceLabel differs after re-login")
	}
}
