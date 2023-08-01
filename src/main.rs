use anyhow::Error;
use common::ConnectionFn;

mod common;
pub use crate::common::args::Args;
mod engines;
pub use crate::engines::impala::Impala;

fn main() -> Result<(), Error> {
    let a = common::Args::parse();

    let i = Impala::new(a.connection_string);
    let mut c = i.execute(&a.query, 1)?;

    let mut res = i.fetch(&mut c, 1)?;
    println!("{:?}", res);
    // let mut res2 = ;
    // println!("{:?}", res2);
    res.append(&mut i.fetch(&mut c, 2)?);
    println!("{:?}", res);
    // println!("{:?}", i.get_res_buffer());

    Ok(())
}
