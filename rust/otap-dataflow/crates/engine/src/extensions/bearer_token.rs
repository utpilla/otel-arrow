// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Bearer token provider handle for extensions.
//!
//! This module defines a token provider contract for extensions that manage
//! bearer authentication tokens (e.g., Azure Managed Identity, OAuth2 flows):
//!
//! - [`BearerTokenProvider`] — trait for components that read cached tokens.
//! - [`BearerTokenProviderHandle`] — a cloneable handle that consumers use to
//!   obtain tokens and subscribe to refresh events.
//!
//! # Design
//!
//! Handle methods are **sync** by design. The extension's background task
//! (`start()`) handles all async I/O — token acquisition, retries, refresh
//! scheduling — and broadcasts updates via a `watch` channel. Consumers only
//! read cached state through the handle; they never perform I/O.
//!
//! This matches the Go Collector's auth extension interfaces, where
//! `Server.Authenticate`, `HTTPClient.RoundTripper`, and
//! `GRPCClient.PerRPCCredentials` are all sync.
//!
//! # Examples
//!
//! ## Extension factory — registering the handle
//!
//! ```rust,ignore
//! let (token_tx, token_rx) = watch::channel(None);
//!
//! let provider = MyTokenProvider { token_rx };
//! let mut handles = ExtensionHandles::new();
//! handles.register(BearerTokenProviderHandle::new(provider));
//! ```
//!
//! ## Exporter — obtaining a cached token
//!
//! ```rust,ignore
//! let handle = extension_registry
//!     .get::<BearerTokenProviderHandle>("my_auth")?;
//!
//! let token = handle.get_token()?;
//! request.headers_mut().insert(
//!     http::header::AUTHORIZATION,
//!     format!("Bearer {}", token.token.secret()).parse().unwrap(),
//! );
//! ```
//!
//! ## Subscribing to token refresh events
//!
//! ```rust,ignore
//! let handle = extension_registry
//!     .get::<BearerTokenProviderHandle>("my_auth")?;
//!
//! let mut token_rx = handle.subscribe_token_refresh();
//! loop {
//!     tokio::select! {
//!         _ = token_rx.changed() => {
//!             if let Some(token) = token_rx.borrow().as_ref() {
//!                 // Update headers, etc.
//!             }
//!         }
//!     }
//! }
//! ```

use std::borrow::Cow;
use std::fmt;
use std::sync::{Arc, Mutex};

// ─── Secret ────────────────────────────────────────────────────────────────

/// Represents a secret value that should not be exposed in logs or debug output.
///
/// The [`Debug`] implementation redacts the actual value.
#[derive(Clone, Eq)]
pub struct Secret(Cow<'static, str>);

impl Secret {
    /// Creates a new `Secret`.
    #[must_use]
    pub fn new<T>(value: T) -> Self
    where
        T: Into<Cow<'static, str>>,
    {
        Self(value.into())
    }

    /// Returns the secret value.
    #[must_use]
    pub fn secret(&self) -> &str {
        &self.0
    }
}

impl PartialEq for Secret {
    fn eq(&self, other: &Self) -> bool {
        self.secret() == other.secret()
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&'static str> for Secret {
    fn from(value: &'static str) -> Self {
        Self::new(value)
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret")
    }
}

// ─── BearerToken ───────────────────────────────────────────────────────────

/// Represents a bearer token with its expiration time.
///
/// The token value is wrapped in [`Secret`] to prevent accidental exposure
/// in logs or debug output.
#[derive(Debug, Clone)]
pub struct BearerToken {
    /// The token value.
    pub token: Secret,
    /// The expiration time as a UNIX timestamp (seconds since epoch).
    pub expires_on: i64,
}

impl BearerToken {
    /// Creates a new bearer token.
    #[must_use]
    pub fn new<T>(token: T, expires_on: i64) -> Self
    where
        T: Into<Secret>,
    {
        Self {
            token: token.into(),
            expires_on,
        }
    }
}

// ─── Error ─────────────────────────────────────────────────────────────────

/// An error returned by bearer token provider operations.
#[derive(Debug, Clone)]
pub struct BearerTokenError {
    /// A human-readable description of the failure.
    pub message: String,
}

impl fmt::Display for BearerTokenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bearer token error: {}", self.message)
    }
}

impl std::error::Error for BearerTokenError {}

// ─── Trait ─────────────────────────────────────────────────────────────────

/// A trait for reading cached bearer authentication tokens.
///
/// Handle methods are **sync** — the extension's background task handles
/// all async I/O (token acquisition, retries, refresh scheduling) and
/// broadcasts updates via a `watch` channel. Implementations read from
/// that channel; they never perform I/O.
pub trait BearerTokenProvider: Send {
    /// Returns the latest cached token.
    ///
    /// # Errors
    ///
    /// Returns a [`BearerTokenError`] if no token is available yet
    /// (e.g., the extension hasn't completed its first refresh).
    fn get_token(&self) -> Result<BearerToken, BearerTokenError>;

    /// Returns a receiver for token refresh notifications.
    ///
    /// Each call creates an independent subscription. The receiver always
    /// contains the latest token value (or `None` if no token has been
    /// acquired yet).
    fn subscribe_token_refresh(&self) -> tokio::sync::watch::Receiver<Option<BearerToken>>;
}

// ─── Handle ────────────────────────────────────────────────────────────────

/// A cloneable handle that consumers use to obtain bearer tokens.
///
/// Wraps any [`BearerTokenProvider`] behind an `Arc<Mutex<…>>`. The `Mutex`
/// makes the handle `Sync` (required by tonic services) without requiring
/// `Sync` on the trait itself. The lock is never contended — methods are
/// sync and complete in nanoseconds (reading from a `watch::Receiver`).
#[derive(Clone)]
pub struct BearerTokenProviderHandle {
    inner: Arc<Mutex<Box<dyn BearerTokenProvider>>>,
}

impl BearerTokenProviderHandle {
    /// Creates a new handle wrapping the given provider implementation.
    pub fn new(provider: impl BearerTokenProvider + 'static) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Box::new(provider))),
        }
    }

    /// Returns the latest cached token.
    ///
    /// Delegates to the underlying [`BearerTokenProvider`] implementation.
    pub fn get_token(&self) -> Result<BearerToken, BearerTokenError> {
        self.inner
            .lock()
            .expect("BearerTokenProvider lock poisoned")
            .get_token()
    }

    /// Returns a receiver for token refresh notifications.
    ///
    /// Delegates to the underlying [`BearerTokenProvider`] implementation.
    pub fn subscribe_token_refresh(&self) -> tokio::sync::watch::Receiver<Option<BearerToken>> {
        self.inner
            .lock()
            .expect("BearerTokenProvider lock poisoned")
            .subscribe_token_refresh()
    }
}

impl fmt::Debug for BearerTokenProviderHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BearerTokenProviderHandle").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::watch;

    /// A trivial in-memory token provider backed by a watch channel.
    struct WatchTokenProvider {
        token_rx: watch::Receiver<Option<BearerToken>>,
    }

    impl BearerTokenProvider for WatchTokenProvider {
        fn get_token(&self) -> Result<BearerToken, BearerTokenError> {
            self.token_rx.borrow().clone().ok_or(BearerTokenError {
                message: "token not yet available".into(),
            })
        }

        fn subscribe_token_refresh(&self) -> watch::Receiver<Option<BearerToken>> {
            self.token_rx.clone()
        }
    }

    #[test]
    fn get_token_returns_error_when_empty() {
        let (_tx, rx) = watch::channel(None);
        let handle = BearerTokenProviderHandle::new(WatchTokenProvider { token_rx: rx });

        let err = handle.get_token().unwrap_err();
        assert!(err.message.contains("not yet available"));
    }

    #[test]
    fn get_token_returns_cached_value() {
        let (_tx, rx) = watch::channel(Some(BearerToken::new("my-token", 1_700_000_000)));
        let handle = BearerTokenProviderHandle::new(WatchTokenProvider { token_rx: rx });

        let token = handle.get_token().unwrap();
        assert_eq!(token.token.secret(), "my-token");
        assert_eq!(token.expires_on, 1_700_000_000);
    }

    #[test]
    fn get_token_sees_updates() {
        let (tx, rx) = watch::channel(None);
        let handle = BearerTokenProviderHandle::new(WatchTokenProvider { token_rx: rx });

        assert!(handle.get_token().is_err());

        let _ = tx.send(Some(BearerToken::new("v1", 100)));
        let token = handle.get_token().unwrap();
        assert_eq!(token.token.secret(), "v1");

        let _ = tx.send(Some(BearerToken::new("v2", 200)));
        let token = handle.get_token().unwrap();
        assert_eq!(token.token.secret(), "v2");
    }

    #[tokio::test]
    async fn subscribe_receives_updates() {
        let (tx, rx) = watch::channel(None);
        let handle = BearerTokenProviderHandle::new(WatchTokenProvider { token_rx: rx });

        let mut sub_rx = handle.subscribe_token_refresh();

        let _ = tx.send(Some(BearerToken::new("refreshed", 200)));
        sub_rx.changed().await.unwrap();

        let refreshed = sub_rx.borrow().clone().unwrap();
        assert_eq!(refreshed.token.secret(), "refreshed");
        assert_eq!(refreshed.expires_on, 200);
    }

    #[test]
    fn secret_debug_does_not_leak() {
        let s = Secret::new("super-secret-value");
        assert_eq!(format!("{:?}", s), "Secret");
    }

    #[test]
    fn secret_equality() {
        let a = Secret::new("same");
        let b = Secret::new("same");
        let c = Secret::new("different");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn bearer_token_from_string() {
        let token = BearerToken::new("my-token".to_string(), 42);
        assert_eq!(token.token.secret(), "my-token");
        assert_eq!(token.expires_on, 42);
    }
}
