//! Shared application state handed to every handler.

use std::sync::Arc;

use deadpool_postgres::Pool;

use crate::config::Settings;

#[derive(Clone)]
pub struct AppState {
    pub pool: Pool,
    pub settings: Arc<Settings>,
}
