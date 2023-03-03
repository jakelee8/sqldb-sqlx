mod mssql;
mod mysql;
mod postgres;

use async_trait::async_trait;
use sqlx::{
    any::AnyConnectionKind, database::HasArguments, query::Query, AnyConnection, Column as _,
    Database, Row, TypeInfo,
};
use wasmcloud_interface_sqldb::{Column, ExecuteResult, QueryResult, Statement};

use crate::result::Result;

pub use self::mssql::*;
pub use self::mysql::*;
pub use self::postgres::*;

#[async_trait]
pub trait SqlDbExecutor {
    async fn execute(&mut self, stmt: &Statement) -> Result<ExecuteResult>;

    async fn fetch_all(&mut self, stmt: &Statement) -> Result<QueryResult>;
}

#[async_trait]
impl SqlDbExecutor for AnyConnection {
    async fn execute(&mut self, stmt: &Statement) -> Result<ExecuteResult> {
        match self.private_get_mut() {
            AnyConnectionKind::Postgres(conn) => conn.execute(&stmt).await,
            AnyConnectionKind::MySql(conn) => conn.execute(&stmt).await,
            AnyConnectionKind::Mssql(conn) => conn.execute(&stmt).await,
        }
    }

    async fn fetch_all(&mut self, stmt: &Statement) -> Result<QueryResult> {
        match self.private_get_mut() {
            AnyConnectionKind::Postgres(conn) => conn.fetch_all(&stmt).await,
            AnyConnectionKind::MySql(conn) => conn.fetch_all(&stmt).await,
            AnyConnectionKind::Mssql(conn) => conn.fetch_all(&stmt).await,
        }
    }
}

pub trait BindCbor
where
    Self: Sized,
{
    fn bind_cbor(self, value: &[u8]) -> Result<Self>;
}

pub(crate) fn bind_query<'a, DB>(
    stmt: &'a Statement,
) -> Result<Query<'a, DB, <DB as HasArguments<'a>>::Arguments>>
where
    DB: Database,
    Query<'a, DB, <DB as HasArguments<'a>>::Arguments>: BindCbor,
{
    let mut query = sqlx::query::<DB>(&stmt.sql);
    if let Some(params) = &stmt.parameters {
        for value in params {
            query = query.bind_cbor(&value)?;
        }
    }
    Ok(query)
}

pub(crate) fn to_columns<R>(rows: &[R]) -> Vec<Column>
where
    R: Row,
{
    rows.first()
        .unwrap()
        .columns()
        .iter()
        .map(|column| Column {
            ordinal: column.ordinal() as u32,
            name: column.name().into(),
            db_type: column.type_info().name().into(),
        })
        .collect()
}
