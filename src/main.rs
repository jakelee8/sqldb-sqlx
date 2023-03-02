//! # wasmCloud sqldb-sqlx capability provider
//!
//! Implements the `wasmcloud:sqldb` capability.

mod config;
mod executor;
mod result;

use std::{collections::HashMap, convert::Infallible, sync::Arc};

use sqlx::{any::AnyPool, pool::PoolConnection, Any};
use tokio::sync::RwLock;
use tracing::{info, instrument};
use wasmbus_rpc::provider::prelude::*;
use wasmcloud_interface_sqldb::{ExecuteResult, QueryResult, SqlDb, SqlDbReceiver, Statement};

use crate::executor::SqlDbExecutor;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    provider_main(
        SqlDbProvider::default(),
        Some("SQLDB Postgres Provider".to_string()),
    )?;

    info!("sqldb provider exiting");
    Ok(())
}

#[derive(Default, Clone, Provider)]
#[services(SqlDb)]
struct SqlDbProvider {
    actors: Arc<RwLock<HashMap<String, AnyPool>>>,
}

impl SqlDbProvider {
    async fn acquire_connection(&self, ctx: &Context) -> RpcResult<PoolConnection<Any>> {
        let actor_id = actor_id(ctx)?;
        let rd = self.actors.read().await;

        let pool = rd
            .get(actor_id)
            .ok_or_else(|| RpcError::InvalidParameter(format!("actor not linked:{}", actor_id)))?;

        pool.acquire()
            .await
            .map_err(|err| RpcError::Other(err.to_string()))
    }
}

impl ProviderDispatch for SqlDbProvider {}

#[async_trait]
impl ProviderHandler for SqlDbProvider {
    #[instrument(level = "debug", skip(self), fields(actor_id = %ld.actor_id))]
    async fn put_link(&self, ld: &LinkDefinition) -> RpcResult<bool> {
        let config = config::load_config(ld)?;
        let pool = config::create_pool(config).await?;
        let mut update_map = self.actors.write().await;
        update_map.insert(ld.actor_id.to_string(), pool);
        Ok(true)
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_link(&self, actor_id: &str) {
        let mut aw = self.actors.write().await;
        if let Some(pool) = aw.remove(actor_id) {
            pool.close().await;
        }
    }

    async fn shutdown(&self) -> Result<(), Infallible> {
        let mut aw = self.actors.write().await;
        for (_, pool) in aw.drain() {
            pool.close().await;
        }
        Ok(())
    }
}

fn actor_id(ctx: &Context) -> Result<&String, RpcError> {
    ctx.actor
        .as_ref()
        .ok_or_else(|| RpcError::InvalidParameter("no actor in request".into()))
}

#[async_trait]
impl SqlDb for SqlDbProvider {
    #[instrument(level = "debug", skip_all, fields(actor_id = ?ctx.actor, sql = stmt.sql))]
    async fn execute(&self, ctx: &Context, stmt: &Statement) -> RpcResult<ExecuteResult> {
        let mut conn = self.acquire_connection(ctx).await?;
        match conn.execute(stmt).await {
            Ok(result) => Ok(result),
            Err(err) => Ok(ExecuteResult {
                error: Some(err.into()),
                ..Default::default()
            }),
        }
    }

    #[instrument(level = "debug", skip_all, fields(actor_id = ?ctx.actor, sql = stmt.sql))]
    async fn query(&self, ctx: &Context, stmt: &Statement) -> RpcResult<QueryResult> {
        let mut conn = self.acquire_connection(ctx).await?;
        match conn.fetch_all(stmt).await {
            Ok(result) => Ok(result),
            Err(err) => Ok(QueryResult {
                error: Some(err.into()),
                ..Default::default()
            }),
        }
    }
}
