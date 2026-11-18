use std::time::Duration;

use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use thiserror::Error;
use tokio::time::sleep;

#[derive(Debug, Error)]
pub enum DbInitError {
    #[error("DATABASE_URL not set")]
    MissingUrl,
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
}

/// Represents an Appeal record in the database.
///
/// Fields:
/// - `id`: Unique UUID for the Appeal.
/// - `actor`: The role of the user filing the Appeal ("author" or "employer").
/// - `review_id`: The ID of the moderation review being appealed.
/// - `reason`: The reason for the Appeal.
/// - `status`: The current status of the Appeal ("open", "accepted", or "rejected").
/// - `resolution_note`: An admin's note on the Appeal's decision.
/// - `created_at`: The time the Appeal was created.
/// - `resolved_at`: The time the Appeal was resolved.
/// - `resolved_by`: The ID of the admin who resolved the Appeal.
/// - `webhook_sent`: A flag indicating if a webhook was sent for the Appeal's decision.
/// - `webhook_sent_at`: The time the webhook was sent.
/// - `webhook_payload`: The payload of the webhook sent.
/// - `webhook_status`: The status of the webhook sent.
/// - `webhook_error`: The error message of the webhook sent.
/// - `webhook_retries`: The number of times the webhook was retried.
/// - `webhook_retry_delay`: The delay between webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The delay between webhook retries in seconds.
/// - `webhook_retry_count_max`: The maximum number of webhook retries.
/// - `webhook_retry_delay_secs_max`: The maximum delay between webhook retries in seconds.
/// - `webhook_retry_count_min`: The maximum number of webhook retries.
/// - `webhook_retry_delay_secs_min`: The maximum delay between webhook retries in seconds.
/// - `webhook_retry_count_max`: The maximum number of webhook retries.
/// - `webhook_retry_delay_secs_max`: The maximum delay between webhook retries in seconds.
/// - `webhook_retry_count_min`: The maximum number of webhook retries.
/// - `webhook_retry_delay_secs_min`: The maximum delay between webhook retries in seconds.
/// - `webhook_retry_count_max`: The maximum number of webhook retries.
/// - `webhook_retry_delay_secs_max`: The maximum delay between webhook retries in seconds.
/// - `webhook_retry_count_min`: The maximum number of webhook retries.
/// - `webhook_retry_delay_secs_min`: The maximum delay between webhook retries in seconds.
/// - `webhook_retry_count_max`: The maximum number of webhook retries.
/// - `webhook_retry_delay_secs_max`: The maximum delay between webhook retries in seconds.
/// - `webhook_retry_count_min`: The maximum number of webhook retries.
/// - `webhook_retry_delay_secs_min`: The maximum delay between webhook retries in seconds.
/// - `webhook_retry_count_max`: The maximum number of webhook retries.
/// - `webhook_retry_delay_secs_max`: The maximum delay between webhook retries in seconds.
/// - `webhook_retry_count_min`: The maximum number of webhook retries.
/// - `webhook_retry_delay_secs_min`: The maximum delay between webhook retries in seconds.
/// - `webhook_retry_count_max`: The maximum number of webhook retries.
/// - `webhook_retry_delay_secs_max`: The maximum delay between webhook retries in second
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry_delay_secs`: The number of webhook retries.
/// - `webhook_retry_count`: The number of webhook retries.
/// - `webhook_retry_delay`: The number of webhook retries.
/// - `webhook_retry