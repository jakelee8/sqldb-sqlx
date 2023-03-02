use async_trait::async_trait;
use sqlx::{
    any::AnyConnectionKind,
    database::HasArguments,
    mssql::MssqlRow,
    mysql::MySqlRow,
    postgres::{types::Oid, PgRow},
    query::Query,
    AnyConnection, Column as _, Database, Decode, Mssql, MssqlConnection, MySql, MySqlConnection,
    PgConnection, Postgres, Row, TypeInfo, ValueRef,
};
use time::{
    format_description::well_known::Rfc3339, macros::format_description, Date, OffsetDateTime,
    PrimitiveDateTime, Time,
};
use uuid::Uuid;
use wasmcloud_interface_sqldb::{Column, ExecuteResult, QueryResult, Statement};

use crate::result::{Error, Result};

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

#[async_trait]
impl SqlDbExecutor for PgConnection {
    async fn execute(&mut self, stmt: &Statement) -> Result<ExecuteResult> {
        let query = bind_query(stmt)?;
        let result = sqlx::Executor::execute(self, query).await?;
        Ok(ExecuteResult {
            rows_affected: result.rows_affected(),
            error: None,
        })
    }

    async fn fetch_all(&mut self, stmt: &Statement) -> Result<QueryResult> {
        let query = bind_query(stmt)?;
        let rows = sqlx::Executor::fetch_all(self, query).await?;
        if rows.is_empty() {
            Ok(QueryResult::default())
        } else {
            Ok(QueryResult {
                num_rows: rows.len() as u64,
                columns: to_columns(&rows),
                rows: pgrow_to_cbor(&rows)?,
                error: None,
            })
        }
    }
}

#[async_trait]
impl SqlDbExecutor for MySqlConnection {
    async fn execute(&mut self, stmt: &Statement) -> Result<ExecuteResult> {
        let query = bind_query(stmt)?;
        let result = sqlx::Executor::execute(self, query).await?;
        Ok(ExecuteResult {
            rows_affected: result.rows_affected(),
            error: None,
        })
    }

    async fn fetch_all(&mut self, stmt: &Statement) -> Result<QueryResult> {
        let query = bind_query(stmt)?;
        let rows = sqlx::Executor::fetch_all(self, query).await?;
        if rows.is_empty() {
            Ok(QueryResult::default())
        } else {
            Ok(QueryResult {
                num_rows: rows.len() as u64,
                columns: to_columns(&rows),
                rows: mysql_to_cbor(&rows)?,
                error: None,
            })
        }
    }
}

#[async_trait]
impl SqlDbExecutor for MssqlConnection {
    async fn execute(&mut self, stmt: &Statement) -> Result<ExecuteResult> {
        let query = bind_query(stmt)?;
        let result = sqlx::Executor::execute(self, query).await?;
        Ok(ExecuteResult {
            rows_affected: result.rows_affected(),
            error: None,
        })
    }

    async fn fetch_all(&mut self, stmt: &Statement) -> Result<QueryResult> {
        let query = bind_query(stmt)?;
        let rows = sqlx::Executor::fetch_all(self, query).await?;
        if rows.is_empty() {
            Ok(QueryResult::default())
        } else {
            Ok(QueryResult {
                num_rows: rows.len() as u64,
                columns: to_columns(&rows),
                rows: mssql_to_cbor(&rows)?,
                error: None,
            })
        }
    }
}

pub trait BindCbor
where
    Self: Sized,
{
    fn bind_cbor(self, value: &[u8]) -> Result<Self>;
}

impl<'q> BindCbor for Query<'q, Postgres, <Postgres as HasArguments<'q>>::Arguments> {
    fn bind_cbor(self, value: &[u8]) -> Result<Self> {
        use minicbor::data::Type;

        let mut decoder = minicbor::Decoder::new(value);
        let datatype = decoder.datatype()?;
        let query = match datatype {
            Type::Bool => self.bind(decoder.bool()?),
            // Type::Null => todo!(),
            // Type::Undefined => todo!(),
            Type::U8 => {
                let value = decoder.u8()?;
                if let Ok(value) = i8::try_from(value) {
                    self.bind(value)
                } else {
                    self.bind(value as i16)
                }
            }
            Type::U16 => {
                let value = decoder.u16()?;
                if let Ok(value) = i16::try_from(value) {
                    self.bind(value)
                } else {
                    self.bind(value as i32)
                }
            }
            Type::U32 => {
                let value = decoder.u32()?;
                if let Ok(value) = i32::try_from(value) {
                    self.bind(value)
                } else {
                    self.bind(value as i64)
                }
            }
            Type::U64 => {
                let value = decoder.u64()?;
                if let Ok(value) = i64::try_from(value) {
                    self.bind(value)
                } else {
                    return Err(Error::CborDeU64OutOfRange(value));
                }
            }
            Type::I8 => self.bind(decoder.i8()? as i16),
            Type::I16 => self.bind(decoder.i16()?),
            Type::I32 => self.bind(decoder.i32()?),
            Type::I64 => self.bind(decoder.i64()?),
            Type::Int => {
                let int = decoder.int()?;
                if let Ok(value) = i16::try_from(int) {
                    self.bind(value)
                } else if let Ok(value) = i32::try_from(int) {
                    self.bind(value)
                } else if let Ok(value) = i64::try_from(int) {
                    self.bind(value)
                } else {
                    return Err(Error::CborDeIntOutOfRange(int));
                }
            }
            Type::F16 => self.bind(decoder.f16()?),
            Type::F32 => self.bind(decoder.f32()?),
            Type::F64 => self.bind(decoder.f64()?),
            // Type::Simple => todo!(),
            Type::Bytes => self.bind(decoder.bytes()?.to_vec()),
            // Type::BytesIndef => todo!(),
            Type::String => self.bind(decoder.str()?.to_string()),
            // Type::StringIndef => todo!(),
            // Type::Array => todo!(),
            // Type::ArrayIndef => todo!(),
            // Type::Map => todo!(),
            // Type::MapIndef => todo!(),
            // Type::Tag => todo!(),
            // Type::Break => todo!(),
            // Type::Unknown(_) => todo!(),
            _ => return Err(Error::CborDeType(datatype)),
        };

        Ok(query)
    }
}

impl<'q> BindCbor for Query<'q, MySql, <MySql as HasArguments<'q>>::Arguments> {
    fn bind_cbor(self, value: &[u8]) -> Result<Self> {
        use minicbor::data::Type;

        let mut decoder = minicbor::Decoder::new(value);
        let datatype = decoder.datatype()?;
        let query = match datatype {
            Type::Bool => self.bind(decoder.bool()?),
            // Type::Null => todo!(),
            // Type::Undefined => todo!(),
            Type::U8 => self.bind(decoder.u8()?),
            Type::U16 => self.bind(decoder.u16()?),
            Type::U32 => self.bind(decoder.u32()?),
            Type::U64 => self.bind(decoder.u64()?),
            Type::I8 => self.bind(decoder.i8()?),
            Type::I16 => self.bind(decoder.i16()?),
            Type::I32 => self.bind(decoder.i32()?),
            Type::I64 => self.bind(decoder.i64()?),
            Type::Int => {
                let int = decoder.int()?;
                if int < 0.into() {
                    if let Ok(value) = i8::try_from(int) {
                        self.bind(value)
                    } else if let Ok(value) = i16::try_from(int) {
                        self.bind(value)
                    } else if let Ok(value) = i32::try_from(int) {
                        self.bind(value)
                    } else if let Ok(value) = i64::try_from(int) {
                        self.bind(value)
                    } else {
                        return Err(Error::CborDeIntOutOfRange(int));
                    }
                } else {
                    if let Ok(value) = u8::try_from(int) {
                        self.bind(value)
                    } else if let Ok(value) = u16::try_from(int) {
                        self.bind(value)
                    } else if let Ok(value) = u32::try_from(int) {
                        self.bind(value)
                    } else if let Ok(value) = u64::try_from(int) {
                        self.bind(value)
                    } else {
                        return Err(Error::CborDeIntOutOfRange(int));
                    }
                }
            }
            Type::F16 => self.bind(decoder.f16()?),
            Type::F32 => self.bind(decoder.f32()?),
            Type::F64 => self.bind(decoder.f64()?),
            // Type::Simple => todo!(),
            Type::Bytes => self.bind(decoder.bytes()?.to_vec()),
            // Type::BytesIndef => todo!(),
            Type::String => self.bind(decoder.str()?.to_string()),
            // Type::StringIndef => todo!(),
            // Type::Array => todo!(),
            // Type::ArrayIndef => todo!(),
            // Type::Map => todo!(),
            // Type::MapIndef => todo!(),
            // Type::Tag => todo!(),
            // Type::Break => todo!(),
            // Type::Unknown(_) => todo!(),
            _ => return Err(Error::CborDeType(datatype)),
        };

        Ok(query)
    }
}

impl<'q> BindCbor for Query<'q, Mssql, <Mssql as HasArguments<'q>>::Arguments> {
    fn bind_cbor(self, value: &[u8]) -> Result<Self> {
        use minicbor::data::Type;

        let mut decoder = minicbor::Decoder::new(value);
        let datatype = decoder.datatype()?;
        let query = match datatype {
            Type::Bool => self.bind(decoder.bool()?),
            // Type::Null => todo!(),
            // Type::Undefined => todo!(),
            Type::U8 => {
                let value = decoder.u8()?;
                if let Ok(value) = i8::try_from(value) {
                    self.bind(value)
                } else {
                    self.bind(value as i16)
                }
            }
            Type::U16 => {
                let value = decoder.u16()?;
                if let Ok(value) = i16::try_from(value) {
                    self.bind(value)
                } else {
                    self.bind(value as i32)
                }
            }
            Type::U32 => {
                let value = decoder.u32()?;
                if let Ok(value) = i32::try_from(value) {
                    self.bind(value)
                } else {
                    self.bind(value as i64)
                }
            }
            Type::U64 => {
                let value = decoder.u64()?;
                if let Ok(value) = i64::try_from(value) {
                    self.bind(value)
                } else {
                    return Err(Error::CborDeU64OutOfRange(value));
                }
            }
            Type::I8 => self.bind(decoder.i8()?),
            Type::I16 => self.bind(decoder.i16()?),
            Type::I32 => self.bind(decoder.i32()?),
            Type::I64 => self.bind(decoder.i64()?),
            Type::Int => {
                let int = decoder.int()?;
                if let Ok(value) = i16::try_from(int) {
                    self.bind(value)
                } else if let Ok(value) = i32::try_from(int) {
                    self.bind(value)
                } else if let Ok(value) = i64::try_from(int) {
                    self.bind(value)
                } else {
                    return Err(Error::CborDeIntOutOfRange(int));
                }
            }
            Type::F16 => self.bind(decoder.f16()?),
            Type::F32 => self.bind(decoder.f32()?),
            Type::F64 => self.bind(decoder.f64()?),
            // Type::Simple => todo!(),
            // Type::Bytes => todo!(),
            // Type::BytesIndef => todo!(),
            Type::String => self.bind(decoder.str()?.to_string()),
            // Type::StringIndef => todo!(),
            // Type::Array => todo!(),
            // Type::ArrayIndef => todo!(),
            // Type::Map => todo!(),
            // Type::MapIndef => todo!(),
            // Type::Tag => todo!(),
            // Type::Break => todo!(),
            // Type::Unknown(_) => todo!(),
            _ => return Err(Error::CborDeType(datatype)),
        };

        Ok(query)
    }
}

fn bind_query<'a, DB>(
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

fn to_columns<R>(rows: &[R]) -> Vec<Column>
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

fn pgrow_to_cbor(rows: &[PgRow]) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(rows.len() * 2);
    let mut out = minicbor::Encoder::new(&mut buf);

    out.array(rows.len() as u64)?;
    for row in rows {
        out.array(row.len() as u64)?;

        for column in row.columns() {
            let value_ref = row.try_get_raw(column.ordinal())?;
            if value_ref.is_null() {
                out.null()?;
                continue;
            }

            let type_name = column.type_info().name();
            match type_name {
                "OID" => {
                    let oid = <Oid as Decode<Postgres>>::decode(value_ref)?;
                    out.encode(oid.0)?;
                }

                "BOOL" => {
                    out.encode(<bool as Decode<Postgres>>::decode(value_ref)?)?;
                }

                "\"CHAR\"" => {
                    out.encode(<i8 as Decode<Postgres>>::decode(value_ref)?)?;
                }
                "SMALLINT" | "SMALLSERIAL" | "INT2" => {
                    out.encode(<i16 as Decode<Postgres>>::decode(value_ref)?)?;
                }
                "INT" | "SERIAL" | "INT4" => {
                    out.encode(<i32 as Decode<Postgres>>::decode(value_ref)?)?;
                }
                "BIGINT" | "BIGSERIAL" | "INT8" => {
                    out.encode(<i64 as Decode<Postgres>>::decode(value_ref)?)?;
                }

                "REAL" | "FLOAT4" => {
                    out.encode(<f32 as Decode<Postgres>>::decode(value_ref)?)?;
                }
                "DOUBLE PRECISION" | "FLOAT8" => {
                    out.encode(<f64 as Decode<Postgres>>::decode(value_ref)?)?;
                }

                "VARCHAR" | "CHAR" | "TEXT" | "NAME" => {
                    out.encode(<&str as Decode<Postgres>>::decode(value_ref)?)?;
                }

                "BYTEA" => {
                    out.encode(<&[u8] as Decode<Postgres>>::decode(value_ref)?)?;
                }

                "TIMESTAMP" => {
                    let timestamp = <PrimitiveDateTime as Decode<Postgres>>::decode(value_ref)?;
                    let format =
                        format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
                    let rfc3339 = timestamp.format(format)?;
                    out.encode(rfc3339)?;
                }

                "TIMESTAMPTZ" => {
                    let timestamp = <OffsetDateTime as Decode<Postgres>>::decode(value_ref)?;
                    let rfc3339 = timestamp.format(&Rfc3339)?;
                    out.encode(rfc3339)?;
                }

                "DATE" => {
                    let date = <Date as Decode<Postgres>>::decode(value_ref)?;
                    let format = format_description!("[year]-[month]-[day]");
                    let value = date.format(format)?;
                    out.encode(value)?;
                }

                "TIME" => {
                    let date = <Time as Decode<Postgres>>::decode(value_ref)?;
                    let format = format_description!("[hour]:[minute]:[second]");
                    let value = date.format(format)?;
                    out.encode(value)?;
                }

                "UUID" => {
                    let id = <Uuid as Decode<Postgres>>::decode(value_ref)?;
                    let value = id.as_hyphenated().to_string();
                    out.encode(value)?;
                }

                "JSON" | "JSONB" => {
                    let json = <serde_json::Value as Decode<Postgres>>::decode(value_ref)?;
                    let value = serde_json::to_string(&json)?;
                    out.encode(value)?;
                }

                "NULL" | "VOID" => {
                    out.null()?;
                }

                _ => {
                    return Err(Error::DbType(type_name.into()));
                }
            }
        }
    }

    Ok(buf)
}

fn mysql_to_cbor(rows: &[MySqlRow]) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(rows.len() * 2);
    let mut out = minicbor::Encoder::new(&mut buf);

    out.array(rows.len() as u64)?;
    for row in rows {
        out.array(row.len() as u64)?;

        for column in row.columns() {
            let value_ref = row.try_get_raw(column.ordinal())?;
            if value_ref.is_null() {
                out.null()?;
                continue;
            }

            let type_name = column.type_info().name();
            match type_name {
                "BOOLEAN" => {
                    out.encode(<bool as Decode<MySql>>::decode(value_ref)?)?;
                }

                "TINYINT" => {
                    out.encode(<i8 as Decode<MySql>>::decode(value_ref)?)?;
                }
                "SMALLINT" => {
                    out.encode(<i16 as Decode<MySql>>::decode(value_ref)?)?;
                }
                "INT" => {
                    out.encode(<i32 as Decode<MySql>>::decode(value_ref)?)?;
                }
                "BIGINT" => {
                    out.encode(<i64 as Decode<MySql>>::decode(value_ref)?)?;
                }

                "TINYINT UNSIGNED" => {
                    out.encode(<u8 as Decode<MySql>>::decode(value_ref)?)?;
                }
                "SMALLINT UNSIGNED" => {
                    out.encode(<u16 as Decode<MySql>>::decode(value_ref)?)?;
                }
                "INT UNSIGNED" => {
                    out.encode(<u32 as Decode<MySql>>::decode(value_ref)?)?;
                }
                "BIGINT UNSIGNED" => {
                    out.encode(<u64 as Decode<MySql>>::decode(value_ref)?)?;
                }

                "FLOAT" => {
                    out.encode(<f32 as Decode<MySql>>::decode(value_ref)?)?;
                }
                "DOUBLE" => {
                    out.encode(<f64 as Decode<MySql>>::decode(value_ref)?)?;
                }

                "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" => {
                    out.encode(<&str as Decode<MySql>>::decode(value_ref)?)?;
                }

                "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
                    out.encode(<&[u8] as Decode<MySql>>::decode(value_ref)?)?;
                }

                "DATETIME" => {
                    let timestamp = <PrimitiveDateTime as Decode<MySql>>::decode(value_ref)?;
                    let format =
                        format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
                    let rfc3339 = timestamp.format(format)?;
                    out.encode(rfc3339)?;
                }

                "TIMESTAMP" => {
                    let timestamp = <OffsetDateTime as Decode<MySql>>::decode(value_ref)?;
                    let rfc3339 = timestamp.format(&Rfc3339)?;
                    out.encode(rfc3339)?;
                }

                "DATE" => {
                    let date = <Date as Decode<MySql>>::decode(value_ref)?;
                    let format = format_description!("[year]-[month]-[day]");
                    let value = date.format(format)?;
                    out.encode(value)?;
                }

                "TIME" => {
                    let date = <Time as Decode<MySql>>::decode(value_ref)?;
                    let format = format_description!("[hour]:[minute]:[second]");
                    let value = date.format(format)?;
                    out.encode(value)?;
                }

                "UUID" => {
                    let id = <Uuid as Decode<MySql>>::decode(value_ref)?;
                    let value = id.as_hyphenated().to_string();
                    out.encode(value)?;
                }

                "JSON" => {
                    let json = <serde_json::Value as Decode<MySql>>::decode(value_ref)?;
                    let value = serde_json::to_string(&json)?;
                    out.encode(value)?;
                }

                "NULL" | "VOID" => {
                    out.null()?;
                }

                _ => {
                    return Err(Error::DbType(type_name.into()));
                }
            }
        }
    }

    Ok(buf)
}

fn mssql_to_cbor(rows: &[MssqlRow]) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(rows.len() * 2);
    let mut out = minicbor::Encoder::new(&mut buf);

    out.array(rows.len() as u64)?;
    for row in rows {
        out.array(row.len() as u64)?;

        for column in row.columns() {
            let value_ref = row.try_get_raw(column.ordinal())?;
            if value_ref.is_null() {
                out.null()?;
                continue;
            }

            let type_name = column.type_info().name();
            match type_name {
                "BOOLEAN" => {
                    out.encode(<bool as Decode<Mssql>>::decode(value_ref)?)?;
                }

                "TINYINT" => {
                    out.encode(<i8 as Decode<Mssql>>::decode(value_ref)?)?;
                }
                "SMALLINT" => {
                    out.encode(<i16 as Decode<Mssql>>::decode(value_ref)?)?;
                }
                "INT" => {
                    out.encode(<i32 as Decode<Mssql>>::decode(value_ref)?)?;
                }
                "BIGINT" => {
                    out.encode(<i64 as Decode<Mssql>>::decode(value_ref)?)?;
                }

                "REAL" => {
                    out.encode(<f32 as Decode<Mssql>>::decode(value_ref)?)?;
                }
                "FLOAT" => {
                    out.encode(<f64 as Decode<Mssql>>::decode(value_ref)?)?;
                }

                "CHAR" | "BIGCHAR" | "NCHAR" | "VARCHAR" | "NVARCHAR" | "BIGVARCHAR" => {
                    out.encode(<String as Decode<Mssql>>::decode(value_ref)?)?;
                }

                _ => {
                    return Err(Error::DbType(type_name.into()));
                }
            }
        }
    }

    Ok(buf)
}
