use async_trait::async_trait;
use sqlx::{
    database::HasArguments, mysql::MySqlRow, query::Query, Column, Decode, MySql, MySqlConnection,
    Row, TypeInfo, ValueRef,
};
use time::{
    format_description::well_known::Rfc3339, macros::format_description, Date, OffsetDateTime,
    PrimitiveDateTime, Time,
};
use uuid::Uuid;
use wasmcloud_interface_sqldb::{ExecuteResult, QueryResult, Statement};

use crate::result::{Error, Result};

use super::{bind_query, to_columns, BindCbor, SqlDbExecutor};

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

impl<'q> BindCbor for Query<'q, MySql, <MySql as HasArguments<'q>>::Arguments> {
    fn bind_cbor(self, value: &[u8]) -> Result<Self> {
        use minicbor::data::Type;

        let mut decoder = minicbor::Decoder::new(value);
        let datatype = decoder.datatype()?;
        let query = match datatype {
            Type::Bool => self.bind(decoder.bool()?),
            Type::Null | Type::Undefined => self.bind(None::<bool>),
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
