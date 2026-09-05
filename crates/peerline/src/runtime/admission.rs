//! Transport-agnostic admission machinery.
//!
//! Every peerline transport screens connections the same way: the
//! transport gathers what it knows about a connection into a *facts*
//! struct (`UdsAccept`, `WsAccept`, `IrohAccept` — those stay in the
//! transport crates, because the facts genuinely differ), a [`Policy`]
//! over those facts decides admit-or-refuse, and an admitted connection
//! gets a [`PeerHandler`] to initialize each of its peer sessions. The
//! decision machinery is identical across transports, so it lives here
//! once: a policy is a conjunction of checks, composed with
//! [`Policy::and`], applied by [`Policy::check`], and paired with an
//! initializer by [`Policy::acceptor`].
//!
//! Transports wrap `Policy<TheirAccept>` in a thin newtype
//! (`UdsPolicy`, `WsPolicy`, `IrohPolicy`) whose named constructors
//! build the transport's rules as checks — so `custom`, `and`,
//! `check`, and `acceptor` mean the same thing on every transport, and
//! a fix to the machinery lands everywhere at once.

use std::sync::Arc;

use super::Peer;

/// A type-erased per-connection peer initializer — the closure that
/// registers a service's handlers, after boxing.
pub type PeerHandler = Arc<dyn Fn(&Peer) + Send + Sync + 'static>;

/// One admission check over a transport's accept facts `A`.
/// `Err(reason)` refuses the connection.
pub type Check<A> = Arc<dyn Fn(&A) -> Result<(), String> + Send + Sync + 'static>;

/// A conjunction of admission checks over a transport's accept facts
/// `A`: every check must pass, in the order they were added, and the
/// first refusal wins. A value rather than a closure, so a mount table
/// can carry it.
///
/// An empty policy admits everything — construct it with
/// [`Policy::allow_any`] so the choice is spelled out at the call site.
pub struct Policy<A> {
    checks: Vec<Check<A>>,
}

// Manual impl: `derive(Clone)` would demand `A: Clone`, which the
// `Vec<Arc<_>>` does not need.
impl<A> Clone for Policy<A> {
    fn clone(&self) -> Self {
        Self {
            checks: self.checks.clone(),
        }
    }
}

impl<A> Policy<A> {
    /// The policy with no checks: admit every connection the transport
    /// completes. What that means — and whether it is safe — depends on
    /// the transport; see each transport's own `allow_any`.
    #[must_use]
    pub fn allow_any() -> Self {
        Self { checks: Vec::new() }
    }

    /// A policy that is only `f` — shorthand for
    /// `Policy::allow_any().and(f)`.
    #[must_use]
    pub fn custom<F>(f: F) -> Self
    where
        F: Fn(&A) -> Result<(), String> + Send + Sync + 'static,
    {
        Self::allow_any().and(f)
    }

    /// Additionally require `f` to accept. Composes: every `and` is
    /// kept and all must pass.
    #[must_use]
    pub fn and<F>(mut self, f: F) -> Self
    where
        F: Fn(&A) -> Result<(), String> + Send + Sync + 'static,
    {
        self.checks.push(Arc::new(f));
        self
    }

    /// Apply the policy. `Err(reason)` refuses the connection, with the
    /// first failing check's reason.
    pub fn check(&self, accept: &A) -> Result<(), String> {
        for check in &self.checks {
            check(accept)?;
        }
        Ok(())
    }

    /// Pair this policy with a peer initializer, giving the acceptor
    /// closure a transport's `serve` wants: check the facts, and on
    /// admission hand back the initializer to run on each peer session.
    pub fn acceptor<F>(
        self,
        on_peer: F,
    ) -> impl Fn(&A) -> Result<PeerHandler, String> + Send + Sync + 'static
    where
        F: Fn(&Peer) + Send + Sync + 'static,
        A: 'static,
    {
        let on_peer: PeerHandler = Arc::new(on_peer);
        move |accept: &A| {
            self.check(accept)?;
            Ok(on_peer.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_policy_admits() {
        assert!(Policy::<u32>::allow_any().check(&7).is_ok());
    }

    #[test]
    fn checks_conjoin_in_order_and_first_refusal_wins() {
        let p = Policy::<u32>::allow_any()
            .and(|n| {
                if *n > 10 {
                    Ok(())
                } else {
                    Err("too small".into())
                }
            })
            .and(|n| {
                if *n % 2 == 0 {
                    Ok(())
                } else {
                    Err("odd".into())
                }
            });
        assert!(p.check(&12).is_ok());
        assert_eq!(p.check(&3).unwrap_err(), "too small");
        assert_eq!(p.check(&13).unwrap_err(), "odd");
    }

    #[test]
    fn custom_is_one_conjunct_on_an_empty_policy() {
        let p = Policy::<u32>::custom(|n| if *n == 1 { Ok(()) } else { Err("no".into()) });
        assert!(p.check(&1).is_ok());
        assert!(p.check(&2).is_err());
    }
}
