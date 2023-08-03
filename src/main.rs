use anyhow::Error;
use common::ConnectionFn;

mod common;
mod printing;
use common::args::Args;
mod engines;
use engines::impala::Impala;

fn main() -> Result<(), Error> {
    let s = printing::CellSize::new(3, true, true, true, true);
    println!("{:?}", s);
    let s = printing::CellSize::default();
    println!("{:?}", s);

    let sp = printing::CellParams::new("vue".to_string(), s);
    println!("{:?}", sp);

    return Ok(());

    let a = Args::parse();

    let i = Impala::new(a.connection_string);
    let mut c = i.execute(&a.query, 1)?;

    let mut res = i.fetch(&mut c, 1)?;
    println!("{:?}", res);
    // let mut res2 = ;
    // println!("{:?}", res2);
    res.append(&mut i.fetch(&mut c, 2)?);
    println!("{:?}", res);
    // println!("{:?}", i.get_res_buffer());
    let l = printing::Line::new(
        String::from("lol1"),
        String::from("lol2"),
        Vec::new(),
        String::from("lol3"),
    );
    l.print()?;
    Ok(())
}
