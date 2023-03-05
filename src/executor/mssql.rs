use async_trait::async_trait;
use sqlx::{
    database::HasArguments, mssql::MssqlRow, query::Query, Column, Decode, Mssql, MssqlConnection,
    Row, TypeInfo, ValueRef,
};
use wasmcloud_interface_sqldb::{ExecuteResult, QueryResult, Statement};

use crate::result::{Error, Result};

use super::{bind_query, to_columns, BindCbor, SqlDbExecutor};

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

impl<'q> BindCbor for Query<'q, Mssql, <Mssql as HasArguments<'q>>::Arguments> {
    fn bind_cbor(self, value: &[u8]) -> Result<Self> {
        use minicbor::data::Type;

        let mut decoder = minicbor::Decoder::new(value);
        let datatype = decoder.datatype()?;
        let query = match datatype {
            Type::Bool => self.bind(decoder.bool()?),
            Type::Null | Type::Undefined => self.bind(None::<bool>),
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
