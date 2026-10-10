//! Where the fetcher is allowed to connect.
//!
//! Until now every URL came from a person: a seed they typed, or a link on a
//! page they chose to crawl. Federated discovery inverts that — the frontier
//! gets fed by third-party APIs, so "fetch this URL" becomes an instruction
//! from an outside party, and `http://169.254.169.254/` in a search result must
//! not be honoured.
//!
//! Checking the hostname before the request is not enough. A name resolves
//! later, can resolve to something different the second time (DNS rebinding),
//! and a redirect can move the request somewhere the caller never named. So the
//! check lives at the only two places that see a real destination: the resolver
//! — whose answer is exactly what the connector then dials — and the URL itself
//! when it names an address literally and no resolution happens at all.

use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, OnceLock};
use url::{Host, Url};

/// Set to `1`, `true` or `yes` to permit private, loopback and link-local
/// destinations. Deliberate intranet use on a single-user install is a real
/// case; it just should not be the default.
pub const ALLOW_PRIVATE_ENV: &str = "MCPCRAWLER_ALLOW_PRIVATE_NETWORKS";

/// Comma-separated hosts exempt from the private-network refusal, and only
/// those. `MCPCRAWLER_ALLOW_HOSTS=localhost,searxng.lan` lets the crawler reach
/// a self-hosted search engine without opening the whole intranet, which is
/// what `MCPCRAWLER_ALLOW_PRIVATE_NETWORKS=1` does.
pub const ALLOW_HOSTS_ENV: &str = "MCPCRAWLER_ALLOW_HOSTS";

/// Appears in every message the resolver gives for a refused destination, so
/// a caller can tell a refusal from a network failure without matching prose.
pub const REFUSAL_MARK: &str = "non-public addresses";

/// Redirect hops allowed. Each one is re-checked, so this only bounds work.
const MAX_REDIRECTS: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetPolicy {
    /// Whether non-public destinations may be reached.
    pub allow_private: bool,
}

impl NetPolicy {
    pub fn strict() -> Self {
        Self { allow_private: false }
    }

    /// For the test suite, whose fixture server is on loopback by design.
    pub fn permissive() -> Self {
        Self { allow_private: true }
    }

    pub fn from_env() -> Self {
        let allow = std::env::var(ALLOW_PRIVATE_ENV)
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(false);
        if allow { Self::permissive() } else { Self::strict() }
    }

    /// Reject a URL that names a destination outright. A hostname cannot be
    /// judged here — that is the resolver's job — but an address literal can,
    /// and it never reaches the resolver at all.
    pub fn check_url(&self, url: &Url) -> Result<(), String> {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(format!("scheme {} is not fetchable", url.scheme()));
        }
        if self.allow_private {
            return Ok(());
        }
        let ip = match url.host() {
            Some(Host::Ipv4(ip)) => IpAddr::V4(ip),
            Some(Host::Ipv6(ip)) => IpAddr::V6(ip),
            Some(Host::Domain(_)) => return Ok(()),
            None => return Err("URL has no host".to_string()),
        };
        if is_public(ip) || host_allowed(&ip.to_string()) {
            Ok(())
        } else {
            Err(format!("{ip} is not a public address"))
        }
    }
}

static ALLOWED_HOSTS: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

/// Exempt one host — a name or an address — from the private-network refusal.
/// Matched exactly and case-insensitively: allowing `localhost` does not allow
/// `evil.localhost.example`, nor `127.0.0.2`.
pub fn allow_host(host: &str) {
    let host = host.trim().trim_matches(['[', ']']).to_ascii_lowercase();
    if host.is_empty() {
        return;
    }
    let mut hosts = ALLOWED_HOSTS.lock().unwrap();
    if !hosts.contains(&host) {
        hosts.push(host);
    }
}

pub fn host_allowed(host: &str) -> bool {
    let host = host.trim().trim_matches(['[', ']']).to_ascii_lowercase();
    ALLOWED_HOSTS.lock().unwrap().contains(&host)
}

/// Read the allowlist from the environment.
pub fn init_allowed_hosts_from_env() {
    if let Ok(list) = std::env::var(ALLOW_HOSTS_ENV) {
        for host in list.split(',') {
            allow_host(host);
        }
    }
}

/// Process-wide policy. This is genuinely one setting for the whole process —
/// threading it through every fetch signature would buy nothing a single-user
/// server can use. Set once at startup; the strict path is the default if it is
/// never set at all.
static POLICY: OnceLock<NetPolicy> = OnceLock::new();

/// Install the policy. Later calls are ignored, so the first writer wins.
pub fn init(policy: NetPolicy) {
    let _ = POLICY.set(policy);
}

pub fn policy() -> NetPolicy {
    *POLICY.get_or_init(NetPolicy::from_env)
}

/// Public unicast, and nothing else. Everything rejected here is either
/// unroutable on the internet or reachable only from inside the host's own
/// network — which is precisely what an outside party must not be able to aim
/// this crawler at.
pub fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_public_v4(ip),
        IpAddr::V6(ip) => is_public_v6(ip),
    }
}

fn is_public_v4(ip: Ipv4Addr) -> bool {
    let [a, b, ..] = ip.octets();
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        // 169.254.0.0/16 — carries the cloud metadata endpoint 169.254.169.254.
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_broadcast()
        || ip.is_documentation()
        // 100.64.0.0/10, carrier-grade NAT: someone else's private space.
        || (a == 100 && (64..128).contains(&b))
        // 192.0.0.0/24 protocol assignments, 198.18.0.0/15 benchmarking.
        || (a == 192 && b == 0 && ip.octets()[2] == 0)
        || (a == 198 && (b == 18 || b == 19))
        // 240.0.0.0/4 reserved.
        || a >= 240)
}

fn is_public_v6(ip: Ipv6Addr) -> bool {
    // An IPv4-mapped address is an IPv4 destination wearing a v6 costume, and
    // ::ffff:127.0.0.1 must be refused for the same reason 127.0.0.1 is.
    if let Some(v4) = ip.to_ipv4_mapped() {
        return is_public_v4(v4);
    }
    let s = ip.segments();
    let embedded = |hi: u16, lo: u16| {
        let [a, b] = hi.to_be_bytes();
        let [c, d] = lo.to_be_bytes();
        Ipv4Addr::new(a, b, c, d)
    };
    // Transition addresses carry an IPv4 destination inside them, and a gateway
    // that translates them will deliver to it: 64:ff9b::/96 (NAT64) holds it in
    // the last 32 bits, 2002::/16 (6to4) in the next 32 after the prefix.
    if s[0] == 0x0064 && s[1] == 0xff9b && s[2..6] == [0; 4] {
        return is_public_v4(embedded(s[6], s[7]));
    }
    if s[0] == 0x2002 {
        return is_public_v4(embedded(s[1], s[2]));
    }
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_multicast()
        // 0000::/8 is reserved, and holds the deprecated IPv4-compatible form
        // (::a.b.c.d) as well as ::1 and ::.
        || (s[0] & 0xff00) == 0
        // fc00::/7 unique local, fe80::/10 link local.
        || (s[0] & 0xfe00) == 0xfc00
        || (s[0] & 0xffc0) == 0xfe80
        // 2001::/32 Teredo, a tunnel rather than a destination; 2001:db8::/32
        // documentation.
        || (s[0] == 0x2001 && (s[1] == 0 || s[1] == 0x0db8)))
}

/// A resolver that hands the connector only addresses it is allowed to dial.
/// Because reqwest connects to exactly what comes back from here, filtering at
/// this point also closes DNS rebinding: there is no second, unchecked lookup
/// between the decision and the connection.
#[derive(Debug)]
pub struct GuardedResolver {
    allow_private: bool,
}

impl Resolve for GuardedResolver {
    fn resolve(&self, name: Name) -> Resolving {
        // A host the operator named is exempt; everything else it resolves to
        // is judged as before.
        let allow_private = self.allow_private || host_allowed(name.as_str());
        let host = name.as_str().to_string();
        Box::pin(async move {
            // Port 0 — the connector substitutes the real one.
            let resolved: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), 0))
                .await
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("could not resolve {host}: {e}").into()
                })?
                .collect();

            if allow_private {
                return Ok(Box::new(resolved.into_iter()) as Addrs);
            }

            let (public, blocked): (Vec<_>, Vec<_>) =
                resolved.into_iter().partition(|addr| is_public(addr.ip()));

            // Partial filtering rather than all-or-nothing: a name with one
            // public and one private address is still fetchable, but only at
            // the address that was allowed.
            if public.is_empty() {
                let detail = blocked
                    .first()
                    .map(|a| a.ip().to_string())
                    .unwrap_or_else(|| "no addresses".to_string());
                return Err(format!(
                    "{host} resolves only to {REFUSAL_MARK} ({detail}); set \
                     {ALLOW_PRIVATE_ENV}=1 to allow intranet destinations"
                )
                .into());
            }
            Ok(Box::new(public.into_iter()) as Addrs)
        })
    }
}

/// Bound the hop count and re-check every hop. The resolver covers hops that
/// name a host; this covers the ones that name an address, and stops a redirect
/// chain from walking out of the scheme.
pub fn redirect_policy(policy: NetPolicy) -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(move |attempt| {
        if attempt.previous().len() >= MAX_REDIRECTS {
            return attempt.stop();
        }
        match policy.check_url(attempt.url()) {
            Ok(()) => attempt.follow(),
            Err(reason) => attempt.error(format!("refused redirect: {reason}")),
        }
    })
}

pub fn resolver(policy: NetPolicy) -> Arc<GuardedResolver> {
    Arc::new(GuardedResolver { allow_private: policy.allow_private })
}
