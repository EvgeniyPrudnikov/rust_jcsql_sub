
use crate::common::eng::Engines;

#[derive(Debug)]
pub struct Args {
    pub engine: Engines,
    pub query: String,
    pub connection_string: String,
}

impl Args {
    pub fn new() -> Self {
        Args {
            engine: match get_nth_arg(1).to_lowercase().as_str() {
                "impala" => Engines::Impala,
                "oracle" => Engines::Oracle,
                "snowflake" => Engines::Snowflake,
                _ => panic!("Engine is not supported"),
            },
            query: get_nth_arg(2),
            connection_string: get_nth_arg(3),
        }
    }
}

impl Default for Args {
    fn default() -> Self {
        Self::new()
    }
}

fn get_nth_arg(n: usize) -> String {
    std::env::args().nth(n).unwrap()
}
