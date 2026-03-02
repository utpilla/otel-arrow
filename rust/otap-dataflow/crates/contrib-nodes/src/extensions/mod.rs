// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extension implementations for contrib nodes.

/// Azure Identity authentication extension using `azure_identity` credentials.
#[cfg(feature = "azure-identity-auth-extension")]
pub mod azure_identity_auth;

/// Bearer Token authentication extension for validating incoming requests.
#[cfg(feature = "bearer-token-auth-extension")]
pub mod bearer_token_auth;
