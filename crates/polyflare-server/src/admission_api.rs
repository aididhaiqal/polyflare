//! Live admission-limit tuning: `GET`/`PATCH /api/admission-limits`.
//!
//! Until 2026-07-29 the admission caps were startup-env only, so tuning them under load meant a
//! restart — which drains every in-flight turn, i.e. the one cost you cannot pay at exactly the
//! moment tuning matters (the 07-28 saturation: every account pinned at its ordinary pressure
//! limit, 90-second owner-waits, an attempt budget burned by a *local* queue timeout). The
//! priority presence policy already steers per-session spend live; this makes the throughput caps
//! it interacts with live too, so the two governors can actually be coordinated.
//!
//! PATCH is partial: only the fields present change, everything else keeps its current value.
//! Each applied value is persisted as a settings row (`admission_<field>`) so it survives a
//! restart — the boot path overlays those rows onto the env-derived limits
//! (`overlay_admission_settings`). Timeouts (`wait_timeout`/`socket_wait_timeout`) are
//! deliberately NOT editable here: they shape client-visible latency contracts, not capacity, and
//! nothing in the incident record needed them moved at runtime.
//!
//! Content-free: counts only — no account ids, no request data.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;

use crate::app::AppState;
use crate::runtime_state::AdmissionLimits;

/// One editable field: its key suffix (settings-row key is `admission_<suffix>`), getter, setter.
type FieldSpec = (
    &'static str,
    fn(&AdmissionLimits) -> u32,
    fn(&mut AdmissionLimits, u32),
);

/// The editable fields, in one place.
const FIELDS: &[FieldSpec] = &[
    (
        "global_in_flight",
        |l| l.global_in_flight,
        |l, v| l.global_in_flight = v,
    ),
    (
        "account_in_flight",
        |l| l.account_in_flight,
        |l, v| l.account_in_flight = v,
    ),
    (
        "global_in_flight_pressure",
        |l| l.global_in_flight_pressure,
        |l, v| l.global_in_flight_pressure = v,
    ),
    (
        "account_in_flight_pressure",
        |l| l.account_in_flight_pressure,
        |l, v| l.account_in_flight_pressure = v,
    ),
    (
        "global_open_ws",
        |l| l.global_open_ws,
        |l, v| l.global_open_ws = v,
    ),
    (
        "account_open_ws",
        |l| l.account_open_ws,
        |l, v| l.account_open_ws = v,
    ),
    (
        "owner_recovery_reserve",
        |l| l.owner_recovery_reserve,
        |l, v| l.owner_recovery_reserve = v,
    ),
    (
        "owner_recovery_pressure_reserve",
        |l| l.owner_recovery_pressure_reserve,
        |l, v| l.owner_recovery_pressure_reserve = v,
    ),
];

fn view(limits: &AdmissionLimits) -> serde_json::Value {
    let mut object = serde_json::Map::new();
    for (key, get, _) in FIELDS {
        object.insert((*key).to_string(), serde_json::json!(get(limits)));
    }
    serde_json::Value::Object(object)
}

/// `GET /api/admission-limits` — the limits every admission gate is using right now.
pub async fn get_handler(State(state): State<Arc<AppState>>) -> Response {
    Json(view(&state.runtime.admission_limits())).into_response()
}

/// `PATCH /api/admission-limits` — apply a partial update live and persist it for the next boot.
/// Zero means "unlimited" for caps, exactly as the env variables define it. Reserves are clamped
/// against their caps by `set_admission_limits` (a reserve at or above its cap would starve all
/// new work), so the RESPONSE — the now-effective limits — is what proves what actually applied.
pub async fn patch_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(object) = body.as_object() else {
        return (
            StatusCode::BAD_REQUEST,
            "request body must be a JSON object",
        )
            .into_response();
    };

    // Validate all-or-nothing before applying anything.
    let mut updates: Vec<(usize, u32)> = Vec::new();
    for (raw_key, raw_value) in object {
        let Some(index) = FIELDS.iter().position(|(key, _, _)| key == raw_key) else {
            return (StatusCode::BAD_REQUEST, "unknown admission-limit field").into_response();
        };
        let Some(value) = raw_value.as_u64().and_then(|n| u32::try_from(n).ok()) else {
            return (
                StatusCode::BAD_REQUEST,
                "value must be an integer in u32 range",
            )
                .into_response();
        };
        updates.push((index, value));
    }

    let mut next = state.runtime.admission_limits();
    for (index, value) in &updates {
        (FIELDS[*index].2)(&mut next, *value);
    }
    let applied = state.runtime.set_admission_limits(next);

    // Persist what actually applied (post-clamp), one row per touched field, so a restart boots
    // with the same effective limits the operator observed.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    for (index, _) in &updates {
        let (key, get, _) = &FIELDS[*index];
        if state
            .store
            .settings()
            .set(&format!("admission_{key}"), &get(&applied).to_string(), now)
            .await
            .is_err()
        {
            return (StatusCode::INTERNAL_SERVER_ERROR, "persist failed").into_response();
        }
    }
    Json(view(&applied)).into_response()
}

/// Boot overlay: apply persisted `admission_<field>` settings rows on top of the env-derived
/// limits, so a live PATCH survives a restart. Unparseable rows are skipped — a corrupt settings
/// row must never take admission down; the env-derived value simply stands.
pub fn overlay_admission_settings(
    limits: &mut AdmissionLimits,
    persisted: &HashMap<String, String>,
) {
    for (key, _, set) in FIELDS {
        if let Some(value) = persisted
            .get(&format!("admission_{key}"))
            .and_then(|raw| raw.trim().parse::<u32>().ok())
        {
            set(limits, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlay_applies_known_rows_and_skips_garbage() {
        let mut limits = AdmissionLimits::default();
        let mut persisted = HashMap::new();
        persisted.insert("admission_account_in_flight".to_string(), "9".to_string());
        persisted.insert(
            "admission_account_in_flight_pressure".to_string(),
            "not-a-number".to_string(),
        );
        persisted.insert("unrelated_key".to_string(), "5".to_string());
        overlay_admission_settings(&mut limits, &persisted);
        assert_eq!(limits.account_in_flight, 9, "known row applies");
        assert_eq!(
            limits.account_in_flight_pressure,
            AdmissionLimits::default().account_in_flight_pressure,
            "a garbage row leaves the env-derived value standing"
        );
    }
}
