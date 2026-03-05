// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Azure Identity authentication extension.
//!
//! This extension acquires Azure AD tokens using `azure_identity` credentials
//! (Managed Identity or Developer Tools) and exposes them as a
//! [`ClientAuthenticatorHandle`] so that exporters and other pipeline
//! components can attach `Authorization: Bearer <token>` headers without
//! managing their own token lifecycle.
//!
//! # Token Refresh
//!
//! The extension's [`start`](Extension::start) loop proactively refreshes
//! the token before it expires and broadcasts updates via a
//! [`tokio::sync::watch`] channel.  Consumers call
//! [`ClientAuthenticatorHandle::get_request_metadata`] to pull the latest
//! cached header — the watch channel ensures efficient, lock-free reads.
//!
//! # Example YAML Configuration
//!
//! ```yaml
//! nodes:
//!   azure_auth:
//!     type: "urn:microsoft:extension:azure_identity_auth"
//!     config:
//!       method: managed_identity
//!       scope: "https://monitor.azure.com/.default"
//! ```

use async_trait::async_trait;
use azure_core::credentials::{AccessToken, TokenCredential};
use azure_identity::{
    DeveloperToolsCredential, DeveloperToolsCredentialOptions, ManagedIdentityCredential,
    ManagedIdentityCredentialOptions, UserAssignedId,
};
use linkme::distributed_slice;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::ExtensionFactory;
use otap_df_engine::config::ExtensionConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::ExtensionControlMsg;
use otap_df_engine::error::Error as EngineError;
use otap_df_engine::extension::ExtensionWrapper;
use otap_df_engine::extensions::auth::{AuthError, ClientAuthenticator, ClientAuthenticatorHandle};
use otap_df_engine::extensions::ExtensionHandles;
use otap_df_engine::local::extension::{self as local, ControlChannel, EffectHandler};
use otap_df_engine::node::NodeId;
use otap_df_telemetry::{otel_debug, otel_error, otel_info, otel_warn};
use http::header::HeaderValue;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::watch;

use otap_df_otap::OTAP_EXTENSION_FACTORIES;

/// URN identifying the Azure Identity Auth extension in configuration.
pub const AZURE_IDENTITY_AUTH_URN: &str = "urn:microsoft:extension:azure_identity_auth";

/// Minimum delay between token refresh retry attempts in seconds.
const MIN_RETRY_DELAY_SECS: f64 = 5.0;
/// Maximum delay between token refresh retry attempts in seconds.
const MAX_RETRY_DELAY_SECS: f64 = 30.0;
/// Maximum jitter percentage (±10%) to add to retry delays.
const MAX_RETRY_JITTER_RATIO: f64 = 0.10;
/// Minimum interval between token refresh attempts (10 seconds).
const MIN_TOKEN_REFRESH_INTERVAL_SECS: u64 = 10;
/// Buffer time before token expiry to trigger a refresh.
/// Azure Identity SDK caches tokens internally and won't issue a new token
/// until ~5 minutes before expiry, so we schedule refresh at 295 seconds
/// before expiry.
const TOKEN_EXPIRY_BUFFER_SECS: u64 = 295;

// ─── Configuration ────────────────────────────────────────────────────────

/// Authentication method for Azure credentials.
#[derive(Debug, Deserialize, Clone, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum AuthMethod {
    /// Use Managed Identity (system or user-assigned with client_id).
    #[serde(alias = "msi", alias = "managed_identity")]
    #[default]
    ManagedIdentity,

    /// Use developer tools (Azure CLI, Azure Developer CLI).
    #[serde(alias = "dev", alias = "developer", alias = "cli")]
    Development,
}

/// Configuration for the Azure Identity Auth extension.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Authentication method to use.
    #[serde(default)]
    pub method: AuthMethod,

    /// Client ID for user-assigned managed identity (optional).
    /// Only used when method is ManagedIdentity.
    /// If not provided with ManagedIdentity, system-assigned identity will be used.
    pub client_id: Option<String>,

    /// OAuth scope for token acquisition (defaults to "https://monitor.azure.com/.default").
    #[serde(default = "default_scope")]
    pub scope: String,
}

fn default_scope() -> String {
    "https://monitor.azure.com/.default".to_string()
}

// ─── ClientAuthenticator implementation ───────────────────────────────────

/// A [`ClientAuthenticator`] backed by a cached Azure AD bearer token.
///
/// The extension's background loop refreshes the token and broadcasts it via
/// a [`tokio::sync::watch`] channel.  Consumers (exporters, heartbeat
/// clients, etc.) read the latest cached header on every call to
/// [`get_request_metadata`] using the watch receiver's lock-free borrow.
struct AzureIdentityClientAuth {
    token_rx: watch::Receiver<Option<HeaderValue>>,
}

impl ClientAuthenticator for AzureIdentityClientAuth {
    fn get_request_metadata(
        &self,
    ) -> Result<Vec<(http::HeaderName, HeaderValue)>, AuthError> {
        let header = self.token_rx.borrow().clone();
        match header {
            Some(h) => Ok(vec![(http::header::AUTHORIZATION, h)]),
            None => Err(AuthError {
                message: "Azure AD token not yet available".into(),
            }),
        }
    }
}

// ─── Extension implementation ─────────────────────────────────────────────

/// The Azure Identity Auth extension.
///
/// Runs a background loop that proactively refreshes an Azure AD access token
/// and broadcasts the formatted `Authorization: Bearer <token>` header value
/// via a [`tokio::sync::watch`] channel for consumers to pull via
/// [`ClientAuthenticatorHandle::get_request_metadata`].
struct AzureIdentityAuthExtension {
    credential: Arc<dyn TokenCredential>,
    scope: String,
    token_tx: watch::Sender<Option<HeaderValue>>,
}

impl AzureIdentityAuthExtension {
    /// Acquire a token with retry and exponential backoff.
    async fn get_token(&self) -> Result<AccessToken, String> {
        let mut attempt = 0_i32;
        loop {
            attempt += 1;
            match self
                .credential
                .get_token(
                    &[&self.scope],
                    Some(azure_core::credentials::TokenRequestOptions::default()),
                )
                .await
            {
                Ok(token) => {
                    otel_debug!(
                        "azure_identity_auth.get_token_succeeded",
                        expires_on = %token.expires_on
                    );
                    return Ok(token);
                }
                Err(e) => {
                    otel_warn!(
                        "azure_identity_auth.get_token_failed",
                        attempt = attempt,
                        error = %e
                    );
                }
            }

            // Exponential backoff: 5s, 10s, 20s, 30s (capped)
            let base_delay_secs = MIN_RETRY_DELAY_SECS * 2.0_f64.powi(attempt - 1);
            let capped_delay_secs = base_delay_secs.min(MAX_RETRY_DELAY_SECS);

            // Add jitter: ±10%
            let jitter_range = capped_delay_secs * MAX_RETRY_JITTER_RATIO;
            let jitter = if jitter_range > 0.0 {
                let random_factor = rand::random::<f64>() * 2.0 - 1.0;
                random_factor * jitter_range
            } else {
                0.0
            };

            let delay_secs = (capped_delay_secs + jitter).max(1.0);
            let delay = tokio::time::Duration::from_secs_f64(delay_secs);

            otel_warn!(
                "azure_identity_auth.retry_scheduled",
                delay_secs = %delay_secs
            );
            tokio::time::sleep(delay).await;
        }
    }
}

/// Compute the next token refresh instant based on the token's expiry time.
fn get_next_token_refresh(token: &AccessToken) -> tokio::time::Instant {
    let now = azure_core::time::OffsetDateTime::now_utc();
    let duration_remaining = if token.expires_on > now {
        (token.expires_on - now).unsigned_abs()
    } else {
        std::time::Duration::ZERO
    };

    let token_valid_until = tokio::time::Instant::now() + duration_remaining;
    let next_token_refresh =
        token_valid_until - tokio::time::Duration::from_secs(TOKEN_EXPIRY_BUFFER_SECS);
    std::cmp::max(
        next_token_refresh,
        tokio::time::Instant::now()
            + tokio::time::Duration::from_secs(MIN_TOKEN_REFRESH_INTERVAL_SECS),
    )
}

#[async_trait(?Send)]
impl local::Extension for AzureIdentityAuthExtension {
    async fn start(
        self: Box<Self>,
        mut ctrl_chan: ControlChannel,
        effect_handler: EffectHandler,
    ) -> Result<(), EngineError> {
        effect_handler
            .info(&format!(
                "[AzureIdentityAuth] Starting: scope={}",
                self.scope
            ))
            .await;

        let mut next_token_refresh = tokio::time::Instant::now();

        loop {
            tokio::select! {
                biased;

                msg = ctrl_chan.recv() => {
                    match msg {
                        Ok(ExtensionControlMsg::Shutdown { .. }) => {
                            otel_info!("azure_identity_auth.shutdown");
                            break;
                        }
                        Ok(_) => {} // ignore other control messages
                        Err(_) => break,
                    }
                }

                _ = tokio::time::sleep_until(next_token_refresh) => {
                    match self.get_token().await {
                        Ok(access_token) => {
                            match HeaderValue::from_str(
                                &format!("Bearer {}", access_token.token.secret()),
                            ) {
                                Ok(header) => {
                                    let _ = self.token_tx.send_replace(Some(header));

                                    next_token_refresh = get_next_token_refresh(&access_token);

                                    let refresh_in = next_token_refresh
                                        .saturating_duration_since(tokio::time::Instant::now());
                                    let total_secs = refresh_in.as_secs();
                                    let hours = total_secs / 3600;
                                    let minutes = (total_secs % 3600) / 60;
                                    let seconds = total_secs % 60;

                                    otel_info!(
                                        "azure_identity_auth.token_refresh",
                                        refresh_in =
                                            format!("{}h {}m {}s", hours, minutes, seconds)
                                    );
                                }
                                Err(e) => {
                                    otel_error!(
                                        "azure_identity_auth.header_creation_failed",
                                        error = ?e
                                    );
                                    next_token_refresh = tokio::time::Instant::now()
                                        + tokio::time::Duration::from_secs(
                                            MIN_TOKEN_REFRESH_INTERVAL_SECS,
                                        );
                                }
                            }
                        }
                        Err(e) => {
                            otel_error!(
                                "azure_identity_auth.token_refresh_failed",
                                error = %e
                            );
                            next_token_refresh = tokio::time::Instant::now()
                                + tokio::time::Duration::from_secs(
                                    MIN_TOKEN_REFRESH_INTERVAL_SECS,
                                );
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

// ─── Credential creation ──────────────────────────────────────────────────

fn create_credential(
    config: &Config,
) -> Result<Arc<dyn TokenCredential>, otap_df_config::error::Error> {
    match config.method {
        AuthMethod::ManagedIdentity => {
            let mut options = ManagedIdentityCredentialOptions::default();

            if let Some(client_id) = &config.client_id {
                otel_info!(
                    "azure_identity_auth.credential_type",
                    method = "user_assigned_managed_identity",
                    client_id = %client_id
                );
                options.user_assigned_id = Some(UserAssignedId::ClientId(client_id.clone()));
            } else {
                otel_info!(
                    "azure_identity_auth.credential_type",
                    method = "system_assigned_managed_identity"
                );
            }

            ManagedIdentityCredential::new(Some(options)).map(|c| c as Arc<dyn TokenCredential>).map_err(|e| {
                otap_df_config::error::Error::InvalidUserConfig {
                    error: format!("Failed to create managed identity credential: {e}"),
                }
            })
        }
        AuthMethod::Development => {
            otel_info!(
                "azure_identity_auth.credential_type",
                method = "developer_tools"
            );
            DeveloperToolsCredential::new(Some(DeveloperToolsCredentialOptions::default())).map(|c| c as Arc<dyn TokenCredential>).map_err(
                |e| otap_df_config::error::Error::InvalidUserConfig {
                    error: format!("Failed to create developer tools credential: {e}"),
                },
            )
        }
    }
}

// ─── Factory registration ─────────────────────────────────────────────────

/// Register the Azure Identity Auth extension with the OTAP pipeline factory.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_EXTENSION_FACTORIES)]
pub static AZURE_IDENTITY_AUTH_EXTENSION: ExtensionFactory = ExtensionFactory {
    name: AZURE_IDENTITY_AUTH_URN,
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

        // Validate scope is non-empty
        if cfg.scope.is_empty() {
            return Err(otap_df_config::error::Error::InvalidUserConfig {
                error: "auth scope must be non-empty".to_string(),
            });
        }

        // Create the Azure credential
        let credential = create_credential(&cfg)?;

        // Create watch channel: extension broadcasts token updates,
        // authenticator reads the latest value via the receiver.
        let (token_tx, token_rx) = watch::channel(None);

        // Build the ClientAuthenticator handle for consumers
        let auth = AzureIdentityClientAuth { token_rx };
        let mut handles = ExtensionHandles::new();
        handles.register(ClientAuthenticatorHandle::new(auth));

        // Build the extension instance
        let extension = AzureIdentityAuthExtension {
            credential,
            scope: cfg.scope,
            token_tx,
        };

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
    use azure_core::credentials::TokenRequestOptions;
    use azure_core::time::OffsetDateTime;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct MockCredential {
        token: String,
        expires_in: azure_core::time::Duration,
        call_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl TokenCredential for MockCredential {
        async fn get_token(
            &self,
            _scopes: &[&str],
            _options: Option<TokenRequestOptions<'_>>,
        ) -> azure_core::Result<AccessToken> {
            let _ = self.call_count.fetch_add(1, Ordering::SeqCst);
            Ok(AccessToken {
                token: self.token.clone().into(),
                expires_on: OffsetDateTime::now_utc() + self.expires_in,
            })
        }
    }

    fn mock_credential(
        token: &str,
        expires_in: azure_core::time::Duration,
    ) -> (Arc<dyn TokenCredential>, Arc<AtomicUsize>) {
        let call_count = Arc::new(AtomicUsize::new(0));
        let cred: Arc<dyn TokenCredential> = Arc::new(MockCredential {
            token: token.to_string(),
            expires_in,
            call_count: call_count.clone(),
        });
        (cred, call_count)
    }

    #[test]
    fn test_urn_constant() {
        assert_eq!(
            AZURE_IDENTITY_AUTH_URN,
            "urn:microsoft:extension:azure_identity_auth"
        );
    }

    #[test]
    fn test_client_auth_returns_none_before_refresh() {
        let (_tx, rx) = watch::channel(None);
        let auth = AzureIdentityClientAuth { token_rx: rx };

        let err = auth.get_request_metadata().unwrap_err();
        assert!(err.message.contains("not yet available"));
    }

    #[test]
    fn test_client_auth_returns_cached_header() {
        let (_tx, rx) = watch::channel(Some(
            HeaderValue::from_static("Bearer test-token"),
        ));
        let auth = AzureIdentityClientAuth { token_rx: rx };

        let metadata = auth.get_request_metadata().unwrap();
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata[0].0, http::header::AUTHORIZATION);
        assert_eq!(metadata[0].1, "Bearer test-token");
    }

    #[test]
    fn test_client_auth_sees_updates() {
        let (tx, rx) = watch::channel(None);
        let auth = AzureIdentityClientAuth { token_rx: rx };

        assert!(auth.get_request_metadata().is_err());

        let _ = tx.send_replace(Some(HeaderValue::from_static("Bearer v1")));
        let meta = auth.get_request_metadata().unwrap();
        assert_eq!(meta[0].1, "Bearer v1");

        let _ = tx.send_replace(Some(HeaderValue::from_static("Bearer v2")));
        let meta = auth.get_request_metadata().unwrap();
        assert_eq!(meta[0].1, "Bearer v2");
    }

    #[test]
    fn test_get_next_token_refresh_far_future() {
        let now = OffsetDateTime::now_utc();
        let expires_on = now + azure_core::time::Duration::seconds(3600);
        let token = AccessToken {
            token: "secret".into(),
            expires_on,
        };

        let refresh_at = get_next_token_refresh(&token);
        let duration_until_refresh = refresh_at.duration_since(tokio::time::Instant::now());

        // Should be ~3600 - 295 = 3305 seconds
        let expected = 3305.0;
        let actual = duration_until_refresh.as_secs_f64();
        assert!(
            (actual - expected).abs() < 5.0,
            "Expected ~{expected}, got {actual}"
        );
    }

    #[test]
    fn test_get_next_token_refresh_already_expired() {
        let now = OffsetDateTime::now_utc();
        let expires_on = now - azure_core::time::Duration::seconds(100);
        let token = AccessToken {
            token: "expired".into(),
            expires_on,
        };

        let refresh_at = get_next_token_refresh(&token);
        let delay = refresh_at.duration_since(tokio::time::Instant::now());

        // Should be at least MIN_TOKEN_REFRESH_INTERVAL_SECS
        assert!(delay.as_secs() >= MIN_TOKEN_REFRESH_INTERVAL_SECS - 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_extension_refreshes_token() {
        use otap_df_engine::config::ExtensionConfig;
        use otap_df_engine::extensions::ExtensionHandles;
        use otap_df_engine::testing::test_node;
        use otap_df_telemetry::reporter::MetricsReporter;
        use std::time::Duration;

        let (credential, call_count) =
            mock_credential("my-azure-token", azure_core::time::Duration::seconds(3600));

        let (token_tx, token_rx) = watch::channel(None);

        let extension = AzureIdentityAuthExtension {
            credential,
            scope: "https://monitor.azure.com/.default".to_string(),
            token_tx,
        };

        let config = ExtensionConfig::new("test_azure_auth");
        let user_config = Arc::new(NodeUserConfig::new_receiver_config("test_ext"));
        let ext = ExtensionWrapper::local(
            extension,
            ExtensionHandles::new(),
            test_node("test_azure_auth"),
            user_config,
            &config,
        );

        let sender = ext.control_sender();
        let (_metrics_rx, metrics_reporter) = MetricsReporter::create_new_and_receiver(1);

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let handle = tokio::task::spawn_local(async move {
                    ext.start(metrics_reporter).await.expect("extension failed");
                });

                // Wait for the first token refresh
                tokio::time::sleep(Duration::from_millis(100)).await;

                // Verify token was acquired
                assert!(call_count.load(Ordering::SeqCst) >= 1);

                // Verify token is broadcast via watch channel
                let header = token_rx.borrow().clone();
                assert!(header.is_some());
                assert_eq!(header.unwrap(), "Bearer my-azure-token");

                // Shutdown
                sender
                    .send(ExtensionControlMsg::Shutdown {
                        deadline: std::time::Instant::now(),
                        reason: "test".to_owned(),
                    })
                    .await
                    .expect("send failed");

                tokio::time::timeout(Duration::from_secs(2), handle)
                    .await
                    .expect("extension did not shut down in time")
                    .expect("join error");
            })
            .await;
    }

    #[test]
    fn test_config_deserialization_defaults() {
        let json = serde_json::json!({});
        let cfg: Config = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.method, AuthMethod::ManagedIdentity);
        assert_eq!(cfg.scope, "https://monitor.azure.com/.default");
        assert!(cfg.client_id.is_none());
    }

    #[test]
    fn test_config_deserialization_development() {
        let json = serde_json::json!({
            "method": "development",
            "scope": "https://custom.scope/.default"
        });
        let cfg: Config = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.method, AuthMethod::Development);
        assert_eq!(cfg.scope, "https://custom.scope/.default");
    }

    #[test]
    fn test_config_deserialization_aliases() {
        for alias in ["msi", "managed_identity"] {
            let json = serde_json::json!({ "method": alias });
            let cfg: Config = serde_json::from_value(json).unwrap();
            assert_eq!(cfg.method, AuthMethod::ManagedIdentity);
        }
        for alias in ["dev", "developer", "cli"] {
            let json = serde_json::json!({ "method": alias });
            let cfg: Config = serde_json::from_value(json).unwrap();
            assert_eq!(cfg.method, AuthMethod::Development);
        }
    }
}
