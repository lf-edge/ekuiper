// Copyright 2025 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package httpx

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

// narcted for IPv4 -> NAT64 (RFC 6052) address
func nat64For(ipv4 string) string {
	b := net.ParseIP(ipv4).To4()
	addr := make(net.IP, 16)
	addr[0], addr[1] = 0x00, 0x64
	addr[2], addr[3] = 0xff, 0x9b
	addr[12], addr[13], addr[14], addr[15] = b[0], b[1], b[2], b[3]
	return addr.String()
}

// 6to4For maps an IPv4 -> 6to4 (RFC 3056) address
func sixToFourFor(ipv4 string) string {
	b := net.ParseIP(ipv4).To4()
	addr := make(net.IP, 16)
	addr[0], addr[1] = 0x20, 0x02
	addr[2], addr[3], addr[4], addr[5] = b[0], b[1], b[2], b[3]
	return addr.String()
}

// teredoFor maps an IPv4 -> Teredo (RFC 4380) address. The client IPv4 octets
// are bit-inverted.
func teredoFor(ipv4 string) string {
	b := net.ParseIP(ipv4).To4()
	addr := make(net.IP, 16)
	addr[0], addr[1] = 0x20, 0x01 // 2001::/32
	addr[2], addr[3] = 0, 0
	addr[4], addr[5], addr[6], addr[7] = 0x41, 0x36, 0xe3, 0x78 // arbitrary server
	addr[8], addr[9] = 0x80, 0x00                               // flags
	addr[10], addr[11] = 0x63, 0xbf                             // client port
	addr[12], addr[13], addr[14], addr[15] = ^b[0], ^b[1], ^b[2], ^b[3]
	return addr.String()
}

// TestIsInternalIP_SSRFTransitionAddresses guards against the IPv6 transition
// address bypass described in GHSA-4q5r-r938-8xr2. The previously-bypassed
// addresses must now be reported as internal.
func TestIsInternalIP_SSRFTransitionAddresses(t *testing.T) {
	internal := []struct {
		label string
		ip    string
	}{
		// IPv4 ranges that were already blocked.
		{"ipv4 loopback", "127.0.0.1"},
		{"ipv4 private 10", "10.0.0.1"},
		{"ipv4 private 172", "172.16.0.1"},
		{"ipv4 private 192", "192.168.1.1"},
		{"ipv4 link-local metadata", "169.254.169.254"},
		{"ipv4 unspecified", "0.0.0.0"},
		{"ipv4 multicast", "224.0.0.1"},
		{"ipv4 cgnat", "100.64.0.1"},
		{"ipv4 cgnat edge low", "100.64.0.0"},
		{"ipv4 cgnat edge high", "100.127.255.255"},
		// IPv6 native ranges that were already blocked.
		{"ipv6 loopback", "::1"},
		{"ipv6 private ula", "fc00::1"},
		{"ipv6 link-local", "fe80::1"},
		{"ipv6 unspecified", "::"},
		// NAT64 (RFC 6052) wrapping an internal IPv4.
		{"nat64 loopback", nat64For("127.0.0.1")},
		{"nat64 metadata", nat64For("169.254.169.254")},
		{"nat64 private", nat64For("10.0.0.1")},
		// NAT64 local-use (RFC 8215) prefix is blocked wholesale; the next
		// case uses the *real* RFC 6052 /48 layout (v4 in bytes 6-7/9-10, u in
		// byte 8, suffix in bytes 11-15) so the embedded IPv4 is 169.254.169.254
		// while the low 32 bits read as 8.8.8.8. The previous implementation
		// decoded the low 32 bits and let this through.
		{"nat64 local-use /48 bypass", "64:ff9b:1:a9fe:a9:fe00:808:808"},
		// Any address under 64:ff9b:1::/48 is blocked (local-use, not globally
		// reachable), including one whose low 32 bits are a public IPv4.
		{"nat64 local-use low-32 public", "64:ff9b:1::808:808"},
		// 6to4 (RFC 3056) wrapping an internal IPv4.
		{"6to4 loopback", sixToFourFor("127.0.0.1")},
		{"6to4 metadata", sixToFourFor("169.254.169.254")},
		{"6to4 private", sixToFourFor("10.0.0.1")},
		// Teredo (RFC 4380) wrapping an internal IPv4.
		{"teredo loopback", teredoFor("127.0.0.1")},
		{"teredo metadata", teredoFor("169.254.169.254")},
		// IPv4-compatible (RFC 4291, deprecated).
		{"ipv4-compatible private", "::10.0.0.1"},
		{"ipv4-compatible loopback", "::127.0.0.1"},
		{"ipv4-compatible metadata", "::169.254.169.254"},
		// IPv4-mapped wrapping an internal IPv4.
		{"ipv4-mapped loopback", "::ffff:127.0.0.1"},
		{"ipv4-mapped metadata", "::ffff:169.254.169.254"},
		{"ipv4-mapped cgnat", "::ffff:100.64.0.1"},
	}
	for _, tt := range internal {
		t.Run(tt.label, func(t *testing.T) {
			ip := net.ParseIP(tt.ip)
			assert.NotNil(t, ip, "failed to parse %s", tt.ip)
			assert.True(t, isInternalIP(ip), "%s (%s) must be blocked", tt.label, tt.ip)
		})
	}
}

func TestIsInternalIP_PublicAddressesAllowed(t *testing.T) {
	public := []struct {
		label string
		ip    string
	}{
		{"ipv4 public dns", "8.8.8.8"},
		{"ipv4 public cloudflare", "1.1.1.1"},
		{"ipv4 just below cgnat", "100.63.255.255"},
		{"ipv4 just above cgnat", "100.128.0.0"},
		// NAT64 wrapping a *public* IPv4 must be allowed: reaching a public
		// address through NAT64 is not an SSRF issue.
		{"nat64 public dns", nat64For("8.8.8.8")},
		// 6to4 wrapping a public IPv4.
		{"6to4 public dns", sixToFourFor("8.8.8.8")},
		// Teredo wrapping a public IPv4.
		{"teredo public dns", teredoFor("8.8.8.8")},
		// Regular public IPv6.
		{"ipv6 public", "2606:4700:4700::1111"},
	}
	for _, tt := range public {
		t.Run(tt.label, func(t *testing.T) {
			ip := net.ParseIP(tt.ip)
			assert.NotNil(t, ip, "failed to parse %s", tt.ip)
			assert.False(t, isInternalIP(ip), "%s (%s) must not be blocked", tt.label, tt.ip)
		})
	}
}

func TestIsInternalIP_Nil(t *testing.T) {
	assert.False(t, isInternalIP(nil))
	assert.False(t, isInternalIP(net.IP{}))
}

func TestEmbeddedIPv4(t *testing.T) {
	tests := []struct {
		name   string
		ip     string
		want   string
		wantOK bool
	}{
		{"nat64 /96", nat64For("169.254.169.254"), "169.254.169.254", true},
		{"6to4", sixToFourFor("8.8.8.8"), "8.8.8.8", true},
		{"teredo", teredoFor("127.0.0.1"), "127.0.0.1", true},
		{"ipv4-compatible", "::10.0.0.1", "10.0.0.1", true},
		{"not transition", "2606:4700:4700::1111", "", false},
		{"ipv4-mapped is handled by to4, not here", "::ffff:8.8.8.8", "", false},
		// RFC 8215 local-use is blocked wholesale by isInternalIP, never decoded.
		{"nat64 local-use /48", "64:ff9b:1:a9fe:a9:fe00:808:808", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip16 := net.ParseIP(tt.ip).To16()
			got, ok := embeddedIPv4(ip16)
			assert.Equal(t, tt.wantOK, ok)
			if ok {
				assert.Equal(t, tt.want, got.String())
			}
		})
	}
}
