use crate::{
    auth::ACCESS_TOKEN_TTL_SECS,
    error::AppError,
    middleware::auth::AuthClaims,
    models::api::{LoginRequest, LoginResponse, MeResponse, RefreshResponse},
    state::AppState,
};
use axum::{
    extract::State,
    http::{
        header::{COOKIE, SET_COOKIE},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
    Json,
};

const COOKIE_NAME: &str = "astra_refresh";
const COOKIE_PATH: &str = "/api/v1/auth";

// ── helpers ───────────────────────────────────────────────────────────────────

fn require_auth(
    state: &AppState,
) -> Result<&std::sync::Arc<crate::services::AuthService>, AppError> {
    state.auth_service.as_ref().ok_or_else(|| {
        AppError::BadRequest(
            "authentication is disabled; set ASTRA_JWT_SECRET to enable".to_string(),
        )
    })
}

fn set_refresh_cookie(token: &str) -> Result<HeaderValue, AppError> {
    let value = format!(
        "{}={}; HttpOnly; SameSite=Strict; Secure; Path={}; Max-Age={}",
        COOKIE_NAME,
        token,
        COOKIE_PATH,
        crate::auth::REFRESH_TOKEN_TTL_DAYS * 86_400,
    );
    HeaderValue::from_str(&value)
        .map_err(|_| AppError::Internal("failed to build refresh cookie".to_string()))
}

fn clear_refresh_cookie() -> HeaderValue {
    HeaderValue::from_static(
        "astra_refresh=; HttpOnly; SameSite=Strict; Secure; Path=/api/v1/auth; Max-Age=0",
    )
}

fn extract_refresh_cookie(headers: &HeaderMap) -> Option<String> {
    headers
        .get(COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| {
            s.split(';')
                .map(str::trim)
                .find(|p| p.starts_with(COOKIE_NAME))
                .and_then(|p| p.strip_prefix(&format!("{}=", COOKIE_NAME)))
                .map(str::to_owned)
        })
}

// ── handlers ──────────────────────────────────────────────────────────────────

/// `POST /api/v1/auth/login`
///
/// Accepts `{ email, password }`. Routes to local or LDAP auth based on the
/// user's `auth_source`. Returns an access token in the body and a refresh
/// token in an `HttpOnly` cookie.
pub async fn login(
    State(state): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> Result<Response, AppError> {
    let auth = require_auth(&state)?;

    let (access_token, refresh_token) = auth
        .login(&req.email, &req.password)
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;

    let body = Json(LoginResponse {
        access_token,
        token_type: "Bearer".to_string(),
        expires_in: ACCESS_TOKEN_TTL_SECS as u64,
    });

    let cookie = set_refresh_cookie(&refresh_token)?;
    let mut response = (StatusCode::OK, body).into_response();
    response.headers_mut().insert(SET_COOKIE, cookie);
    Ok(response)
}

/// `POST /api/v1/auth/refresh`
///
/// Reads the `astra_refresh` cookie and issues a new access token.
pub async fn refresh(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<RefreshResponse>, AppError> {
    let auth = require_auth(&state)?;

    let refresh_token = extract_refresh_cookie(&headers)
        .ok_or_else(|| AppError::BadRequest("missing refresh token cookie".to_string()))?;

    let access_token = auth
        .refresh(&refresh_token)
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;

    Ok(Json(RefreshResponse {
        access_token,
        token_type: "Bearer".to_string(),
        expires_in: ACCESS_TOKEN_TTL_SECS as u64,
    }))
}

/// `POST /api/v1/auth/logout`
///
/// Revokes the refresh token from the `astra_refresh` cookie and clears it.
pub async fn logout(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let auth = require_auth(&state)?;

    let refresh_token = extract_refresh_cookie(&headers)
        .ok_or_else(|| AppError::BadRequest("missing refresh token cookie".to_string()))?;

    auth.logout(&refresh_token)
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;

    let mut response = StatusCode::NO_CONTENT.into_response();
    response
        .headers_mut()
        .insert(SET_COOKIE, clear_refresh_cookie());
    Ok(response)
}

/// `GET /api/v1/auth/me`
///
/// Returns the current user's profile, derived from the JWT claims injected by
/// the auth middleware. Works in dev mode (returns synthetic dev-admin info).
pub async fn me(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
) -> Result<Json<MeResponse>, AppError> {
    // Dev mode: no auth_service, return synthetic profile.
    let Some(auth) = state.auth_service.as_ref() else {
        return Ok(Json(MeResponse {
            id: claims.sub,
            email: claims.email,
            auth_source: "dev".to_string(),
        }));
    };

    let user = auth
        .get_user(claims.sub)
        .await?
        .ok_or_else(|| AppError::Internal("authenticated user not found in store".to_string()))?;

    Ok(Json(MeResponse {
        id: user.id,
        email: user.email,
        auth_source: user.auth_source,
    }))
}
