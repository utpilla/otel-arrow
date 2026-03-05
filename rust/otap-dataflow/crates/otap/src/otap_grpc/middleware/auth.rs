// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! gRPC middleware for server-side authentication.
//!
//! This middleware integrates [`ServerAuthenticatorHandle`] with the tonic
//! gRPC server via the `tonic_middleware` crate.  When an auth extension is
//! configured, incoming requests are validated before they reach the signal
//! handler; unauthenticated requests receive a gRPC `UNAUTHENTICATED` status.
//! When no auth is configured the middleware is a transparent pass-through.

use async_trait::async_trait;
use http::{Request, Response};
use otap_df_engine::extensions::auth::ServerAuthenticatorHandle;
use tonic::body::Body;
use tonic_middleware::{Middleware, ServiceBound};

/// A tonic middleware that optionally validates incoming requests using a
/// [`ServerAuthenticatorHandle`].
///
/// When `auth` is `Some`, every request is checked against the authenticator
/// and rejected with gRPC `UNAUTHENTICATED` on failure.  When `auth` is
/// `None` the middleware is a no-op pass-through, which avoids type-level
/// branching in the server builder.
#[derive(Clone)]
pub struct AuthMiddleware {
    auth: Option<ServerAuthenticatorHandle>,
}

impl AuthMiddleware {
    /// Creates a new auth middleware.
    ///
    /// Pass `Some(handle)` to enforce authentication, or `None` for a
    /// transparent pass-through.
    pub fn new(auth: Option<ServerAuthenticatorHandle>) -> Self {
        Self { auth }
    }
}

#[async_trait]
impl<S> Middleware<S> for AuthMiddleware
where
    S: ServiceBound,
    S::Future: Send,
{
    async fn call(
        &self,
        req: Request<Body>,
        mut service: S,
    ) -> Result<Response<Body>, S::Error> {
        if let Some(auth) = &self.auth {
            if let Err(e) = auth.authenticate(req.headers()) {
                let status = tonic::Status::unauthenticated(e.message);
                return Ok(status.into_http());
            }
        }

        service.call(req).await
    }
}
