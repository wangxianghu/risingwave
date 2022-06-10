// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

use async_trait::async_trait;
use itertools::{join, Itertools};
use mysql_async::prelude::*;
use mysql_async::*;
use risingwave_common::array::Op::*;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::decimal::Decimal;
use risingwave_common::types::{
    IntervalUnit, NaiveDateWrapper, NaiveTimeWrapper, OrderedF32, OrderedF64, ScalarImpl, Datum
};

#[async_trait]
pub trait Sink {
    async fn write_batch(&mut self, chunk: StreamChunk, schema: &Schema) -> Result<()>;

    fn endpoint(&self) -> String;
    fn table(&self) -> String;
    fn database(&self) -> Option<String>;
    fn user(&self) -> Option<String>;
    fn password(&self) -> Option<String>; // TODO(nanderstabel): auth?
}

// Primitive design of MySQLSink
#[allow(dead_code)]
pub struct MySQLSink {
    endpoint: String,
    table: String,
    database: Option<String>,
    user: Option<String>,
    password: Option<String>,
}

impl MySQLSink {
    pub fn new(
        endpoint: String,
        table: String,
        database: Option<String>,
        user: Option<String>,
        password: Option<String>,
    ) -> Self {
        Self {
            endpoint,
            table,
            database,
            user,
            password,
        }
    }
}

// TODO(nanderstabel): Add DATETIME and TIMESTAMP
pub enum MySQLValue {
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(OrderedF32),
    Double(OrderedF64),
    Bool(bool),
    Decimal(Decimal),
    Varchar(String),
    Date(NaiveDateWrapper),
    Time(NaiveTimeWrapper),
    Interval(IntervalUnit),
    Null
}

impl TryFrom<Datum> for MySQLValue {
    type Error = RwError;

    fn try_from(s: Datum) -> Result<Self> {
        match s {
            Some(ScalarImpl::Int16(v)) => Ok(MySQLValue::SmallInt(v)),
            Some(ScalarImpl::Int32(v)) => Ok(MySQLValue::Int(v)),
            Some(ScalarImpl::Int64(v)) => Ok(MySQLValue::BigInt(v)),
            Some(ScalarImpl::Float32(v)) => Ok(MySQLValue::Float(v)),
            Some(ScalarImpl::Float64(v)) => Ok(MySQLValue::Double(v)),
            Some(ScalarImpl::Bool(v)) => Ok(MySQLValue::Bool(v)),
            Some(ScalarImpl::Decimal(v)) => Ok(MySQLValue::Decimal(v)),
            Some(ScalarImpl::Utf8(v)) => Ok(MySQLValue::Varchar(v)),
            Some(ScalarImpl::NaiveDate(v)) => Ok(MySQLValue::Date(v)),
            Some(ScalarImpl::NaiveTime(v)) => Ok(MySQLValue::Time(v)),
            Some(ScalarImpl::Interval(v)) => Ok(MySQLValue::Interval(v)),
            Some(_) => unimplemented!(),
            None => Ok(MySQLValue::Null),
        }
    }
}

impl fmt::Display for MySQLValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MySQLValue::SmallInt(v) => write!(f, "{}", v)?,
            MySQLValue::Int(v) => write!(f, "{}", v)?,
            MySQLValue::BigInt(v) => write!(f, "{}", v)?,
            MySQLValue::Float(v) => write!(f, "{}", v)?,
            MySQLValue::Double(v) => write!(f, "{}", v)?,
            MySQLValue::Bool(v) => write!(f, "{}", v)?,
            MySQLValue::Decimal(v) => write!(f, "{}", v)?,
            MySQLValue::Varchar(v) => write!(f, "{}", v)?,
            MySQLValue::Date(_v) => todo!(),
            MySQLValue::Time(_v) => todo!(),
            MySQLValue::Interval(_v) => todo!(),
            MySQLValue::Null => write!(f, "NULL")?,
        }
        Ok(())
    }
}

// TODO(nanderstabel): currently writing chunk, not batch..
#[async_trait]
impl Sink for MySQLSink {
    async fn write_batch(&mut self, chunk: StreamChunk, schema: &Schema) -> Result<()> {
        // Closure that takes an idx to create a vector of MySQLValues from a StreamChunk 'row'.
        let values = |idx| -> Result<Vec<MySQLValue>> {
            chunk
                .columns()
                .iter()
                .map(|x| MySQLValue::try_from(x.array_ref().datum_at(idx)))
                .collect_vec()
                .into_iter()
                .collect()  
        };

        // Closure that builds a String containing WHERE conditions, i.e. 'v1=1 AND v2=2'.
        // Perhaps better to replace this functionality with a new Sink trait method.
        let conditions = |values: Vec<MySQLValue>| {
            schema
                .names()
                .iter()
                .zip_eq(values.iter())
                .map(|(c, v)| format!("{}={}", c, v))
                .collect::<Vec<String>>()
        };

        // Build a connection and start transaction
        // TODO(nanderstabel): fix, currently defaults to port 3306
        let builder = OptsBuilder::default()
            .user(self.user())
            .pass(self.password())
            .ip_or_hostname(self.endpoint())
            .db_name(self.database());
        let mut conn = Conn::new(builder).await?;
        let mut transaction = conn.start_transaction(TxOpts::default()).await?;

        let mut iter = chunk.ops().iter().enumerate();
        while let Some((idx, op)) = iter.next() {
            // Get SQL statement
            let stmt = match *op {
                Insert => format!(
                    "INSERT INTO {} VALUES ({});",
                    self.table(),
                    join(values(idx)?, ",")
                ),
                Delete => format!(
                    "DELETE FROM {} WHERE ({});",
                    self.table(),
                    join(conditions(values(idx)?), " AND ")
                ),
                UpdateDelete => {
                    if let Some((idx2, UpdateInsert)) = iter.next() {
                        format!(
                            "UPDATE {} SET {} WHERE {};",
                            self.table,
                            join(conditions(values(idx2)?), ","),
                            join(conditions(values(idx)?), " AND ")
                        )
                    } else {
                        panic!("UpdateDelete should always be followed by an UpdateInsert!")
                    }
                }
                _ => panic!("UpdateInsert should always follow an UpdateDelete!"),
            };
            transaction.exec_drop(stmt, Params::Empty).await?;
        }

        // Commit and drop the connection.
        transaction.commit().await?;
        drop(conn);
        Ok(())
    }

    fn endpoint(&self) -> String {
        self.endpoint.clone()
    }

    fn table(&self) -> String {
        self.table.clone()
    }

    fn database(&self) -> Option<String> {
        self.database.clone()
    }

    fn user(&self) -> Option<String> {
        self.user.clone()
    }

    fn password(&self) -> Option<String> {
        self.password.clone()
    }
}

pub struct RedisSink;

#[async_trait]
impl Sink for RedisSink {
    async fn write_batch(&mut self, _chunk: StreamChunk, _schema: &Schema) -> Result<()> {
        todo!();
    }

    fn endpoint(&self) -> String {
        todo!();
    }

    fn table(&self) -> String {
        todo!();
    }

    fn database(&self) -> Option<String> {
        todo!();
    }

    fn user(&self) -> Option<String> {
        todo!();
    }

    fn password(&self) -> Option<String> {
        todo!();
    }
}

#[cfg(test)]
mod test {
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::Field;
    use risingwave_common::types::DataType;

    use super::*;

    struct ConnectionParams<'a> {
        pub endpoint: &'a str,
        pub table: &'a str,
        pub database: &'a str,
        pub user: &'a str,
        pub password: &'a str,
    }

    // Start connection with optional database.
    async fn start_connection(params: &ConnectionParams<'_>, with_db: bool) -> Result<Conn> {
        let mut builder = OptsBuilder::default()
            .user(Some(params.user))
            .pass(Some(params.password))
            .ip_or_hostname(params.endpoint);
        if with_db {
            builder = builder.db_name(Some(params.database));
        }
        let conn = Conn::new(builder).await?;
        Ok(conn)
    }

    async fn create_database_and_table(
        params: &ConnectionParams<'_>,
        schema: &Schema,
    ) -> Result<()> {
        let mut conn = start_connection(params, false).await?;
        conn.query_drop(format!(
            "CREATE DATABASE {database}; \
            USE {database}; \
            CREATE TABLE {table} ({columns});",
            database = params.database,
            table = params.table,
            columns = join(
                schema
                    .names()
                    .iter()
                    .map(|n| format!("{} int", n))
                    .collect::<Vec<String>>(),
                ", "
            )
        ))
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_basic() -> Result<()> {
        // Connection parameters for testing purposes.
        let params = ConnectionParams {
            endpoint: "127.0.0.1",
            table: "t",
            database: "db",
            user: "root",
            password: "123",
        };

        // Initialize a sink using connection parameters.
        let mut sink = MySQLSink::new(
            params.endpoint.into(),
            params.table.into(),
            Some(params.database.into()),
            Some(params.user.into()),
            Some(params.password.into()),
        );

        // Initialize schema of two Int32 columns.
        let schema = Schema::new(vec![
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v2"),
        ]);

        // Create table using connection parameters and table schema.
        // Beware: currently only works for 'int' type columns.
        create_database_and_table(&params, &schema).await?;

        // Initialize streamchunk.
        let chunk = StreamChunk::from_pretty(
            "  i  i
            + 55 11
            + 44 22
            + 33 00
            - 55 11",
        );

        sink.write_batch(chunk, &schema).await?;

        // Start a new connection using the same connection parameters and get the SELECT result.
        let mut conn = start_connection(&params, true).await?;
        let select: Vec<(i32, i32)> = conn
            .query(format!("SELECT * FROM {};", params.table))
            .await?;

        assert_eq!(select, [(44, 22), (33, 00),]);

        // Initialize streamchunk with UpdateDelete and UpdateInsert.
        let chunk = StreamChunk::from_pretty(
            "   i  i
            U- 44 22
            U+ 44 42",
        );

        sink.write_batch(chunk, &schema).await?;

        let mut conn = start_connection(&params, true).await?;
        let select: Vec<(i32, i32)> = conn
            .query(format!("SELECT * FROM {};", params.table))
            .await?;

        assert_eq!(select, [(44, 42), (33, 00),]);

        // Delete the database and drop the connection.
        conn.query_drop(format!("DROP DATABASE {};", params.database))
            .await?;
        drop(conn);

        Ok(())
    }
}
