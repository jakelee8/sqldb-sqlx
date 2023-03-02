use std::convert::Infallible;

use thiserror::Error;
use wasmcloud_interface_sqldb::SqlDbError;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    CborDe(#[from] minicbor::decode::Error),

    #[error("unsupported CBOR type: `{0}`")]
    CborDeType(minicbor::data::Type),

    #[error("CBOR int value out of range: `{0}`")]
    CborDeIntOutOfRange(minicbor::data::Int),

    #[error("CBOR u64 value out of range: `{0}`")]
    CborDeU64OutOfRange(u64),

    #[error(transparent)]
    CborSer(#[from] minicbor::encode::Error<Infallible>),

    #[error("unsupported database")]
    ConfigDatabaseNotSupported,

    #[error(transparent)]
    Db(#[from] sqlx::Error),

    #[error("unsupported database type: `{0}`")]
    DbType(String),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    #[error(transparent)]
    Sqlx(#[from] sqlx::error::BoxDynError),

    #[error(transparent)]
    TimeFormat(#[from] time::error::Format),
}

impl From<Error> for SqlDbError {
    fn from(err: Error) -> SqlDbError {
        match err {
            Error::ConfigDatabaseNotSupported => SqlDbError::new("config", err.to_string()),
            Error::CborDe(_)
            | Error::CborDeType(_)
            | Error::CborDeIntOutOfRange(_)
            | Error::CborDeU64OutOfRange(_) => SqlDbError::new("decoding", err.to_string()),
            Error::CborSer(_) | Error::SerdeJson(_) | Error::TimeFormat(_) => {
                SqlDbError::new("encoding", err.to_string())
            }
            Error::Db(_) | Error::DbType(_) | Error::Sqlx(_) => {
                SqlDbError::new("db", err.to_string())
            }
        }
    }
}
