use std::{fs::File, io::Read};

use crate::common::eng::Engines;

#[derive(Debug)]
pub struct Args {
    pub engine: Engines,
    pub connection_string: String,
    pub query_file_name: String,
    pub fetch_num: i32,
}

impl Args {
    pub fn parse() -> Self {
        Args {
            engine: match get_nth_arg(1).to_lowercase().as_str() {
                "impala" => Engines::Impala,
                "oracle" => Engines::Oracle,
                "snowflake" => Engines::Snowflake,
                _ => panic!("Engine is not supported"),
            },
            connection_string: get_nth_arg(2),
            query_file_name: get_nth_arg(3),
            fetch_num: get_nth_arg(5).parse::<i32>().unwrap(),
        }
    }

    pub fn get_query(&self) -> String {
        let mut qfile = File::open(&self.query_file_name).unwrap();
        let mut contents = String::new();
        qfile.read_to_string(&mut contents).unwrap();
        contents
    }
}

fn get_nth_arg(n: usize) -> String {
    std::env::args().nth(n).unwrap()
}
