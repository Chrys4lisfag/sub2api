package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// identitySalts segregate the same seed into independent ID namespaces so
// installation/connection/instance never collide.
const (
	saltInstallation = "agymimic/v1/installation"
	saltConnection   = "agymimic/v1/connection"
	saltInstanceHost = "agymimic/v1/instance/host"
	saltInstanceUser = "agymimic/v1/instance/user"
)

// Identity is the trio of machine-identifier fields any one account advertises
// across requests. All three are deterministic functions of the account seed.
type Identity struct {
	InstallationID string // UUID, looks like agy's installation_id file
	ConnectionID   string // UUID, used as Unleash connectionId
	InstanceLabel  string // "DESKTOP-XXXXXXX\<user>-DESKTOP-XXXXXXX"
}

// DeriveIdentity returns a stable Identity bound to `seed`. Same seed in →
// byte-identical Identity out, every time, on any machine. Empty seed falls
// back to random (use only for one-off testing).
//
// For per-account identity in sub2api, pass the account's stable Google email
// (or Google `sub` claim if you have it) as seed. Re-login keeps the same IDs;
// new account → unrelated IDs; collisions across accounts are HMAC-prevented.
func DeriveIdentity(seed string) Identity {
	if seed == "" {
		return Identity{
			InstallationID: NewInstallationID(),
			ConnectionID:   NewInstallationID(),
			InstanceLabel:  NewFakeInstanceLabel(),
		}
	}
	return Identity{
		InstallationID: deriveUUID(seed, saltInstallation),
		ConnectionID:   deriveUUID(seed, saltConnection),
		InstanceLabel:  deriveInstanceLabel(seed),
	}
}

// deriveUUID returns an RFC-4122 v4-shape UUID whose bytes are HMAC(seed, salt).
// Deterministic, account-bound, looks like uuid.NewString().
func deriveUUID(seed, salt string) string {
	mac := hmac.New(sha256.New, []byte(salt))
	mac.Write([]byte(seed))
	sum := mac.Sum(nil) // 32 bytes; we use the first 16
	var u uuid.UUID
	copy(u[:], sum[:16])
	// stamp version (4) + RFC-4122 variant so it parses everywhere
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	return u.String()
}

// deriveInstanceLabel returns the "DESKTOP-XXXXXXX\<user>-DESKTOP-XXXXXXX"
// Unleash instance string. The hostname suffix and username are both seeded
// from `seed` so the same account always advertises the same machine.
func deriveInstanceLabel(seed string) string {
	host := deriveHostname(seed)
	user := deriveUsername(seed)
	return fmt.Sprintf("%s\\%s-%s", host, user, host)
}

func deriveHostname(seed string) string {
	mac := hmac.New(sha256.New, []byte(saltInstanceHost))
	mac.Write([]byte(seed))
	sum := mac.Sum(nil)
	// 7 uppercase alphanumeric chars (Windows default DESKTOP suffix)
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	out := []byte("DESKTOP-")
	for i := 0; i < 7; i++ {
		out = append(out, alphabet[int(sum[i])%len(alphabet)])
	}
	return string(out)
}

func deriveUsername(seed string) string {
	users := []string{
		"admin", "user", "developer", "dev", "alex", "chris", "sam",
		"jordan", "morgan", "taylor", "casey", "drew", "jamie", "kim",
		"pat", "robin", "blake", "skyler", "river", "quinn",
	}
	mac := hmac.New(sha256.New, []byte(saltInstanceUser))
	mac.Write([]byte(seed))
	sum := mac.Sum(nil)
	idx := int(binary.BigEndian.Uint32(sum[:4])) % len(users)
	if idx < 0 {
		idx += len(users)
	}
	return users[idx]
}

// AccountSeed produces the canonical seed for an account from whatever stable
// identifiers we have. Email is the primary; falls back to refresh_token if no
// email is known.
//
// Email is lower-cased & whitespace-trimmed so "Foo@gmail.com" and
// "foo@gmail.com " resolve to the same identity.
func AccountSeed(t *Tokens) string {
	if t == nil {
		return ""
	}
	if e := strings.ToLower(strings.TrimSpace(t.Email)); e != "" {
		return "email:" + e
	}
	if rt := strings.TrimSpace(t.RefreshToken); rt != "" {
		// Hash the refresh token — DON'T leak it directly as a seed.
		h := sha256.Sum256([]byte(rt))
		return "rt:" + hex.EncodeToString(h[:16])
	}
	return ""
}

// EnsureIdentity fills in InstallationID, ConnectionID, InstanceLabel on a
// Tokens whose identity fields are empty. Uses AccountSeed(t) for stability;
// returns true if any field was changed.
func EnsureIdentity(t *Tokens) bool {
	if t == nil {
		return false
	}
	if t.InstallationID != "" && t.ConnectionID != "" && t.InstanceLabel != "" {
		return false
	}
	id := DeriveIdentity(AccountSeed(t))
	changed := false
	if t.InstallationID == "" {
		t.InstallationID = id.InstallationID
		changed = true
	}
	if t.ConnectionID == "" {
		t.ConnectionID = id.ConnectionID
		changed = true
	}
	if t.InstanceLabel == "" {
		t.InstanceLabel = id.InstanceLabel
		changed = true
	}
	return changed
}

// ---- legacy random helpers (used only when no seed is available) ----

// NewInstallationID returns a fresh installation UUID. Prefer
// DeriveIdentity(seed).InstallationID for account-bound stability.
func NewInstallationID() string {
	return uuid.NewString()
}

// NewFakeInstanceLabel returns a random "DESKTOP-XXXXXXX\<user>-…" string.
// Prefer DeriveIdentity(seed).InstanceLabel for account-bound stability.
func NewFakeInstanceLabel() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	suffix := strings.ToUpper(hex.EncodeToString(b))[:7]
	users := []string{"admin", "user", "developer", "dev", "alex", "chris", "sam", "jordan", "morgan", "taylor"}
	ub := make([]byte, 1)
	_, _ = rand.Read(ub)
	user := users[int(ub[0])%len(users)]
	host := "DESKTOP-" + suffix
	return fmt.Sprintf("%s\\%s-%s", host, user, host)
}
