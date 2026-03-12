// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Bearer Token authentication extension (server-side).
//!
//! This extension provides a simple static bearer token check for receivers.
//! Incoming requests must carry an `Authorization: Bearer <token>` header whose
//! token matches the configured value; requests without a valid header are
//! rejected with an [`AuthError`].
//!
//! # Example YAML Configuration
//!
//! ```yaml
//! nodes:
//!   bearer_auth:
//!     type: "urn:otel:extension:bearer_token_auth"
//!     config:
//!       token: "my-secret-token"
//!
//!   otlp-receiver:
//!     type: "urn:otel:receiver:otlp"
//!     config:
//!       auth:
//!         authenticator: "bearer_auth"
//!       protocols:
//!         grpc:
//!           listening_addr: "0.0.0.0:4317"
//! ```

use async_trait::async_trait;
use linkme::distributed_slice;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::ExtensionFactory;
use otap_df_engine::config::ExtensionConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::ExtensionControlMsg;
use otap_df_engine::error::Error as EngineError;
use otap_df_engine::extension::ExtensionWrapper;
use otap_df_engine::extensions::ExtensionHandles;
use otap_df_engine::extensions::auth::{AuthError, ServerAuthenticator, ServerAuthenticatorHandle};
use otap_df_engine::local::extension::{self as local, ControlChannel, EffectHandler};
use otap_df_engine::node::NodeId;
use otap_df_telemetry::otel_info;
use serde::Deserialize;
use std::sync::Arc;

use otap_df_otap::OTAP_EXTENSION_FACTORIES;

/// URN identifying the Bearer Token Auth extension in configuration.
pub const BEARER_TOKEN_AUTH_URN: &str = "urn:otel:extension:bearer_token_auth";

/// Expected prefix in the Authorization header value.
const BEARER_PREFIX: &str = "Bearer ";

// ─── Configuration ────────────────────────────────────────────────────────

/// Configuration for the Bearer Token Auth extension.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The static bearer token that incoming requests must present.
    pub token: String,
}

// ─── ServerAuthenticator implementation ───────────────────────────────────

/// A [`ServerAuthenticator`] that validates a static bearer token.
///
/// Checks the `Authorization` header for a `Bearer <token>` value matching
/// the configured token.
struct BearerTokenServerAuth {
    /// The expected token value (without the "Bearer " prefix).
    expected_token: String,
}

impl ServerAuthenticator for BearerTokenServerAuth {
    fn authenticate(&self, headers: &http::HeaderMap) -> Result<(), AuthError> {
        let auth_header = headers
            .get(http::header::AUTHORIZATION)
            .ok_or_else(|| AuthError {
                message: "missing Authorization header".into(),
            })?;

        let auth_value = auth_header.to_str().map_err(|_| AuthError {
            message: "Authorization header contains invalid characters".into(),
        })?;

        if !auth_value.starts_with(BEARER_PREFIX) {
            return Err(AuthError {
                message: "Authorization header is not a Bearer token".into(),
            });
        }

        let provided_token = &auth_value[BEARER_PREFIX.len()..];
        if provided_token != self.expected_token {
            return Err(AuthError {
                message: "invalid bearer token".into(),
            });
        }

        Ok(())
    }
}

// ─── Extension implementation ─────────────────────────────────────────────

/// The Bearer Token Auth extension.
///
/// This extension has no background work — it simply awaits shutdown.
/// The authentication logic lives in [`BearerTokenServerAuth`] which is
/// registered as a [`ServerAuthenticatorHandle`] for receivers to consume.
struct BearerTokenAuthExtension;

#[async_trait(?Send)]
impl local::Extension for BearerTokenAuthExtension {
    async fn start(
        self: Box<Self>,
        mut ctrl_chan: ControlChannel,
        _effect_handler: EffectHandler,
    ) -> Result<(), EngineError> {
        // No background work — just wait for shutdown.
        loop {
            match ctrl_chan.recv().await {
                Ok(ExtensionControlMsg::Shutdown { .. }) => {
                    otel_info!("bearer_token_auth.shutdown");
                    break;
                }
                Ok(_) => {} // ignore other control messages
                Err(_) => break,
            }
        }
        Ok(())
    }
}

// ─── Factory registration ─────────────────────────────────────────────────

/// Register the Bearer Token Auth extension with the OTAP pipeline factory.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_EXTENSION_FACTORIES)]
pub static BEARER_TOKEN_AUTH_EXTENSION: ExtensionFactory = ExtensionFactory {
    name: BEARER_TOKEN_AUTH_URN,
    create: |_pipeline_ctx: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             extension_config: &ExtensionConfig| {
        // Deserialize user config JSON into typed Config
        let cfg: Config = serde_json::from_value(node_config.config.clone()).map_err(|e| {
            otap_df_config::error::Error::InvalidUserConfig {
                error: e.to_string(),
            }
        })?;

        // Build the ServerAuthenticator handle for receivers
        let auth = BearerTokenServerAuth {
            expected_token: cfg.token,
        };
        let mut handles = ExtensionHandles::new();
        handles.register(ServerAuthenticatorHandle::new(auth));

        // Build the extension instance (no background state needed)
        let extension = BearerTokenAuthExtension;

        Ok(ExtensionWrapper::local(
            extension,
            handles,
            node,
            node_config,
            extension_config,
        ))
    },
    validate_config: otap_df_config::validation::validate_typed_config::<Config>,
};

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use http::HeaderValue;
    use http::header::AUTHORIZATION;

    fn make_auth(token: &str) -> BearerTokenServerAuth {
        BearerTokenServerAuth {
            expected_token: token.to_string(),
        }
    }

    #[test]
    fn valid_token_is_accepted() {
        let auth = make_auth("secret-123");
        let mut headers = HeaderMap::new();
        _ = headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer secret-123"));
        assert!(auth.authenticate(&headers).is_ok());
    }

    #[test]
    fn missing_authorization_header_is_rejected() {
        let auth = make_auth("secret-123");
        let headers = HeaderMap::new();
        let err = auth.authenticate(&headers).unwrap_err();
        assert!(err.message.contains("missing Authorization header"));
    }

    #[test]
    fn wrong_token_is_rejected() {
        let auth = make_auth("correct-token");
        let mut headers = HeaderMap::new();
        _ = headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer wrong-token"),
        );
        let err = auth.authenticate(&headers).unwrap_err();
        assert!(err.message.contains("invalid bearer token"));
    }

    #[test]
    fn non_bearer_scheme_is_rejected() {
        let auth = make_auth("secret");
        let mut headers = HeaderMap::new();
        _ = headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Basic dXNlcjpwYXNz"),
        );
        let err = auth.authenticate(&headers).unwrap_err();
        assert!(err.message.contains("not a Bearer token"));
    }

    #[test]
    fn empty_token_in_header_is_rejected() {
        let auth = make_auth("secret");
        let mut headers = HeaderMap::new();
        _ = headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer "));
        let err = auth.authenticate(&headers).unwrap_err();
        assert!(err.message.contains("invalid bearer token"));
    }

    #[test]
    fn handle_wraps_authenticator() {
        let auth = make_auth("my-token");
        let handle = ServerAuthenticatorHandle::new(auth);

        let mut good = HeaderMap::new();
        _ = good.insert(AUTHORIZATION, HeaderValue::from_static("Bearer my-token"));
        assert!(handle.authenticate(&good).is_ok());

        let bad = HeaderMap::new();
        assert!(handle.authenticate(&bad).is_err());
    }

    #[test]
    fn config_deserializes() {
        let json = serde_json::json!({ "token": "abc-123" });
        let cfg: Config = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.token, "abc-123");
    }

    #[test]
    fn config_rejects_unknown_fields() {
        let json = serde_json::json!({ "token": "abc", "extra": true });
        assert!(serde_json::from_value::<Config>(json).is_err());
    }
}
