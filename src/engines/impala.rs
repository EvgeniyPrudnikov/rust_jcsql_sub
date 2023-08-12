use crate::common::eng::ColDesc;
use crate::common::ConnectionFn;
use crate::common::Engines;

use anyhow::{Error, Ok};
use lazy_static::lazy_static;
use odbc_api::handles::StatementImpl;

use odbc_api::{
    buffers::TextRowSet, Connection, ConnectionOptions, Cursor, CursorImpl, DataType, Environment,
    ResultSetMetadata,
};

const MAX_BATCH_SIZE: usize = 5000;
const MAX_STR_LIMIT: Option<usize> = None;

lazy_static! {
    pub static ref ODBC_ENV: Environment = Environment::new().unwrap();
}

pub struct Impala {
    pub engine: Engines,
    pub connection_string: String,
    connection: Connection<'static>,
    // res_buffer: Vec<Vec<String>>,
}

impl Impala {
    pub fn new(connection_string: String) -> Self {
        let conn = ODBC_ENV
            .connect_with_connection_string(&connection_string, ConnectionOptions::default())
            .expect("Error creating the connection");

        Impala {
            engine: Engines::Impala,
            connection_string,
            connection: conn,
            // res_buffer: Vec::new(),
        }
    }

    // pub fn get_res_buffer(&self) -> &Vec<Vec<String>> {
    //     &self.res_buffer
    // }
}

impl ConnectionFn for Impala {
    type Cursor<'a> = Box<
        odbc_api::BlockCursor<
            CursorImpl<StatementImpl<'a>>,
            odbc_api::buffers::ColumnarBuffer<odbc_api::buffers::TextColumn<u8>>,
        >,
    >;

    fn execute(
        &self,
        q: &str,
        fetch_num_size: i32,
    ) -> Result<(Vec<ColDesc>, Option<Self::Cursor<'_>>), Error> {
        let mut cursor = self.connection.execute(q, ())?.unwrap();

        let mut columns_desc: Vec<ColDesc> = Vec::new();
        let cols_num = cursor.num_result_cols().unwrap();
        for col_idx in 1..=cols_num {
            let col_idx = col_idx as u16;
            columns_desc.push(ColDesc::new(
                usize::from(col_idx - 1),
                cursor.col_name(col_idx)?,
                match cursor.col_data_type(col_idx)? {
                    DataType::Char { .. } => "Char",
                    DataType::WChar { .. } => "Varchar",
                    DataType::Numeric { .. } => "Numeric",
                    DataType::Decimal { .. } => "Decimal",
                    DataType::Integer => "Integer",
                    DataType::SmallInt => "SmallInt",
                    DataType::Float { .. } => "Float",
                    DataType::Real => "Real",
                    DataType::Double => "Double",
                    DataType::Varchar { .. } => "Varchar",
                    DataType::Date => "Date",
                    DataType::Time { .. } => "Time",
                    DataType::Timestamp { .. } => "Timestamp",
                    DataType::BigInt => "BigInt",
                    DataType::TinyInt => "TinyInt",
                    DataType::Bit => "Bit",
                    DataType::Varbinary { .. } => "Varbinary",
                    DataType::Binary { .. } => "Binary",
                    DataType::Other { .. } => "Other",
                    DataType::WVarchar { .. } => "NVarchar",
                    DataType::LongVarchar { .. } => "TEXT",
                    DataType::LongVarbinary { .. } => "BLOB",
                    DataType::Unknown => "Unknown",
                }
                .to_owned(),
            ))
        }

        let buffers = Box::new(TextRowSet::for_cursor(
            {
                if fetch_num_size == -1 || (fetch_num_size as usize) > MAX_BATCH_SIZE {
                    MAX_BATCH_SIZE
                } else {
                    fetch_num_size as usize
                }
            },
            &mut cursor,
            MAX_STR_LIMIT,
        )?);
        let row_set_cursor = Box::new(cursor.bind_buffer(*buffers)?);
        Ok((columns_desc, Some(row_set_cursor)))
    }

    fn fetch(&self, c: &mut Self::Cursor<'_>, fetch_num: i32) -> Result<(Vec<Vec<String>>, bool), Error> {
        let mut res_buffer: Vec<Vec<String>> = Vec::new();
        // Iterate over batches
        let mut fetched = 0;
        while fetched < fetch_num || fetch_num == -1 {
            if let Some(batch) = c.fetch()? {
                // Within a batch, iterate over every row
                fetched += batch.num_rows() as i32;
                for row_index in 0..batch.num_rows() {
                    // Within a row iterate over every column
                    let record = (0..batch.num_cols()).map(|col_index| {
                        batch.at(col_index, row_index).unwrap_or("NULL".as_bytes())
                    });

                    let mut row: Vec<String> = Vec::new();
                    for i in record.into_iter() {
                        row.push(String::from_utf8(i.to_vec()).unwrap());
                    }
                    res_buffer.push(row);
                }
            } else {
                break;
            }
        }
        let fetched_all_rows = fetched == 0 || fetched < fetch_num - 1;
        Ok((res_buffer, fetched_all_rows))
    }
}
