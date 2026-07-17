use super::model::{InventoryConfig, InventoryError};
use super::reader::{InventoryQueryFailure, InventoryQueryStage};
use mysql::DriverError;
use std::time::Duration;

pub(crate) fn is_retryable_inventory_error(error: &mysql::Error) -> bool {
    match error {
        mysql::Error::IoError(_) | mysql::Error::CodecError(_) | mysql::Error::TlsError(_) => true,
        mysql::Error::DriverError(driver_error) => matches!(
            driver_error,
            DriverError::ConnectTimeout
                | DriverError::CouldNotConnect(_)
                | DriverError::PacketOutOfSync
                | DriverError::UnexpectedPacket
                | DriverError::SetupError
                | DriverError::Timeout
        ),
        _ => false,
    }
}

pub(crate) fn log_inventory_connection_reset(
    stage: InventoryQueryStage,
    schema: &str,
    config: &InventoryConfig,
    failure: &InventoryQueryFailure,
) {
    eprintln!(
        "{}",
        format_inventory_reset_log(stage, schema, config, failure)
    );
}

pub(crate) fn format_inventory_reset_log(
    stage: InventoryQueryStage,
    schema: &str,
    config: &InventoryConfig,
    failure: &InventoryQueryFailure,
) -> String {
    format!(
        "cdc_inventory_connection_reset role={} stage={} schema={} attempt=1/2 tls={} reset=true connection_age_ms={} error={}",
        config.endpoint_role.as_str(),
        stage.as_str(),
        schema,
        config.use_tls,
        format_connection_age(failure.connection_age),
        failure.error,
    )
}

pub(crate) fn inventory_attempt_error(
    stage: InventoryQueryStage,
    schema: &str,
    config: &InventoryConfig,
    failure: InventoryQueryFailure,
) -> InventoryError {
    InventoryError::new(format!(
        "inventory query failed role={} stage={} schema={} attempt=1/2 tls={} reset=false connection_age_ms={} error={}",
        config.endpoint_role.as_str(),
        stage.as_str(),
        schema,
        config.use_tls,
        format_connection_age(failure.connection_age),
        failure.error,
    ))
}

pub(crate) fn inventory_retry_error(
    stage: InventoryQueryStage,
    schema: &str,
    config: &InventoryConfig,
    first_failure: InventoryQueryFailure,
    retry_failure: InventoryQueryFailure,
) -> InventoryError {
    InventoryError::new(format!(
        "inventory query failed role={} stage={} schema={} attempt=2/2 tls={} reset=true connection_age_ms={} original_error={} retry_error={}",
        config.endpoint_role.as_str(),
        stage.as_str(),
        schema,
        config.use_tls,
        format_connection_age(retry_failure.connection_age),
        first_failure.error,
        retry_failure.error,
    ))
}

fn format_connection_age(age: Option<Duration>) -> String {
    age.map(|duration| duration.as_millis().to_string())
        .unwrap_or_else(|| "unavailable".to_string())
}
