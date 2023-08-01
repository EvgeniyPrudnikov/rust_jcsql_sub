use crate::common::ConnectionFn;
use crate::common::Engines;

use anyhow::{Error, Ok};
use lazy_static::lazy_static;
use odbc_api::handles::StatementImpl;

use odbc_api::{
    buffers::TextRowSet, Connection, ConnectionOptions, Cursor, CursorImpl, Environment,
    ResultSetMetadata,
};

const BATCH_SIZE: usize = 5000;
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
            .expect("msg");

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

    fn execute(&self, q: &str, fetch_num_size: usize) -> Result<Self::Cursor<'_>, Error> {
        let mut cursor = self.connection.execute(q, ())?.unwrap();

        let headline: Vec<String> = cursor.column_names()?.collect::<Result<_, _>>()?;
        println!("{:?}", headline);

        let buffers = Box::new(TextRowSet::for_cursor(
            {
                if BATCH_SIZE > fetch_num_size {
                    fetch_num_size
                } else {
                    BATCH_SIZE
                }
            },
            &mut cursor,
            MAX_STR_LIMIT,
        )?);
        let row_set_cursor = Box::new(cursor.bind_buffer(*buffers)?);
        Ok(row_set_cursor)
    }

    fn fetch(&self, c: &mut Self::Cursor<'_>, fetch_num: usize) -> Result<Vec<Vec<String>>, Error> {
        let mut res_buffer: Vec<Vec<String>> = Vec::new();
        // Iterate over batches
        let mut fetched: usize = 0;
        while fetched < fetch_num {
            if let Some(batch) = c.fetch()? {
                // Within a batch, iterate over every row
                fetched += batch.num_rows();
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
        Ok(res_buffer)
    }
}
