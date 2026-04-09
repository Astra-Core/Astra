use crate::{auth::Claims, state::AppState};
use axum::{
    async_trait,
    extract::{FromRequestParts, Request, State},
    http::{header::AUTHORIZATION, request::Parts, HeaderMap, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Extractor that pulls the validated [`Claims`] injected by [`auth_middleware`]
/// from request extensions. Handlers that need the caller's identity use this.
// Consumed by HTTP handlers (issue #149); allow until that issue lands.
#[allow(dead_code)]
pub struct AuthClaims(pub Claims);

#[async_trait]
impl<S> FromRequestParts<S> for AuthClaims
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, Json<serde_json::Value>);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<Claims>()
            .cloned()
            .map(AuthClaims)
            .ok_or_else(|| {
                (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({"error": "unauthorized"})),
                )
            })
    }
}

/// Tower/Axum middleware that validates Bearer JWTs and enforces casbin policies.
///
/// Behaviour:
/// - When auth is disabled (no `auth_service` on state): injects synthetic dev-admin
///   claims and passes the request through unchanged.
/// - When auth is enabled:
///   1. Extracts `Authorization: Bearer <token>`.
///   2. Decodes and validates the token via `AuthService::decode_token`.
///   3. Resolves the route to `(object, action)` via [`route_permission`].
///   4. Enforces the casbin policy for the caller's subject.
///   5. On success: inserts [`Claims`] into request extensions and calls `next`.
///   6. On failure: returns `401` or `403`.
pub async fn auth_middleware(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Response {
    // Dev / in-memory mode — auth disabled.
    let Some(auth_service) = state.auth_service.as_ref() else {
        req.extensions_mut().insert(Claims::dev_admin());
        return next.run(req).await;
    };

    // Extract Bearer token from Authorization header.
    let token = match extract_bearer(req.headers()) {
        Some(t) => t,
        None => return err_401("unauthorized"),
    };

    // Decode and validate the JWT.
    let claims = match auth_service.decode_token(&token) {
        Ok(c) => c,
        Err(e) => {
            // Distinguish expired tokens from other decode errors.
            if is_expired(&e) {
                return err_401("token_expired");
            }
            return err_401("unauthorized");
        }
    };

    // Determine the required casbin (object, action) for this route.
    let (obj, act) = route_permission(req.method(), req.uri().path());

    // Enforce the policy when the route is mapped to a specific resource.
    if obj != "*" {
        let allowed = state
            .enforcer
            .enforce(&claims.sub.to_string(), obj, act)
            .await
            .unwrap_or(false);

        if !allowed {
            return err_403("forbidden");
        }
    }

    req.extensions_mut().insert(claims);
    next.run(req).await
}

/// Map an HTTP `(method, path)` pair to a casbin `(object, action)` pair.
///
/// Returns `("*", "*")` for routes that are not mapped to a specific resource;
/// the middleware treats these as pass-through (authentication is still required,
/// but no casbin decision is enforced).
fn route_permission(method: &Method, path: &str) -> (&'static str, &'static str) {
    if path.starts_with("/api/v1/specs/apply") {
        return ("pipelines", "write");
    }

    if path.starts_with("/api/v1/pipelines") {
        return match *method {
            Method::GET => ("pipelines", "read"),
            Method::POST | Method::PUT => ("pipelines", "write"),
            Method::DELETE => ("pipelines", "delete"),
            _ => ("*", "*"),
        };
    }

    if path.starts_with("/api/v1/pipeline-runs") {
        return match *method {
            Method::GET => ("pipelines", "read"),
            Method::POST | Method::PUT => ("pipelines", "write"),
            _ => ("*", "*"),
        };
    }

    if path.starts_with("/api/v1/connections") {
        return match *method {
            Method::GET => ("connections", "read"),
            Method::POST | Method::PUT => ("connections", "write"),
            Method::DELETE => ("connections", "delete"),
            _ => ("*", "*"),
        };
    }

    if path.starts_with("/api/v1/users") {
        return match *method {
            Method::GET => ("users", "read"),
            Method::POST | Method::PUT => ("users", "write"),
            Method::DELETE => ("users", "delete"),
            _ => ("*", "*"),
        };
    }

    ("*", "*")
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn extract_bearer(headers: &HeaderMap) -> Option<String> {
    headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|s| s.to_owned())
}

fn is_expired(err: &anyhow::Error) -> bool {
    if let Some(jwt_err) = err.downcast_ref::<jsonwebtoken::errors::Error>() {
        return matches!(
            jwt_err.kind(),
            jsonwebtoken::errors::ErrorKind::ExpiredSignature
        );
    }
    false
}

fn err_401(message: &'static str) -> Response {
    (StatusCode::UNAUTHORIZED, Json(json!({"error": message}))).into_response()
}

fn err_403(message: &'static str) -> Response {
    (StatusCode::FORBIDDEN, Json(json!({"error": message}))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_routes_map_correctly() {
        assert_eq!(
            route_permission(&Method::GET, "/api/v1/pipelines"),
            ("pipelines", "read")
        );
        assert_eq!(
            route_permission(&Method::DELETE, "/api/v1/pipelines/my-pipe"),
            ("pipelines", "delete")
        );
        assert_eq!(
            route_permission(&Method::POST, "/api/v1/specs/apply"),
            ("pipelines", "write")
        );
    }

    #[test]
    fn connection_routes_map_correctly() {
        assert_eq!(
            route_permission(&Method::GET, "/api/v1/connections"),
            ("connections", "read")
        );
        assert_eq!(
            route_permission(&Method::POST, "/api/v1/connections"),
            ("connections", "write")
        );
        assert_eq!(
            route_permission(&Method::DELETE, "/api/v1/connections/pg"),
            ("connections", "delete")
        );
    }

    #[test]
    fn unmatched_routes_are_wildcard() {
        assert_eq!(
            route_permission(&Method::GET, "/api/v1/unknown"),
            ("*", "*")
        );
    }
}
