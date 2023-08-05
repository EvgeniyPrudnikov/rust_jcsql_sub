use anyhow::Error;
use common::{eng::ColDesc, ConnectionFn};

mod common;
mod printing;
use common::args::Args;
mod engines;
use engines::impala::Impala;

use crate::printing::{CellLines, CellParams, CellSize};

fn main() -> Result<(), Error> {
    let a = Args::parse();

    let i = Impala::new(a.connection_string);
    let (col_des, mut c) = i.execute(&a.query, 100)?;
    let data = i.fetch(&mut c, 100)?;

    println!("{:?}", data);

    let mut col_max: Vec<usize> = vec![0; col_des.len()];

    for i in data.iter() {
        get_data_col_max_lens(i, &mut col_max);
    }
    get_header_col_max_lens(&col_des, &mut col_max);

    let mut print_buffer: Vec<Vec<CellLines>> = Vec::new();
    let mut print_row: Vec<CellLines> = Vec::new();

    for (col_idx, cd) in col_des.iter().enumerate() {
        print_row.push(printing::cell_to_print(CellParams::new(
            cd.get_print_name(),
            CellSize::new(col_max[col_idx], col_idx == 0, true, true, true),
        )));
    }
    print_buffer.push(print_row);

    let res_len = data.len();
    for (row_idx, row) in data.iter().enumerate() {
        let mut print_row: Vec<CellLines> = Vec::new();
        for (col_idx, col) in row.iter().enumerate() {
            print_row.push(printing::cell_to_print(CellParams::new(
                col.to_string(),
                CellSize::new(
                    col_max[col_idx],
                    col_idx == 0,
                    true,
                    false,
                    row_idx == (res_len - 1),
                ),
            )));
        }
        print_buffer.push(print_row);
    }

    for i in print_buffer {
        printing::print_cells_line(i);
    }

    Ok(())
}

fn get_data_col_max_lens(data_row: &[String], max_len: &mut [usize]) {
    for (idx, i) in data_row.iter().enumerate() {
        if i.len() > max_len[idx] {
            max_len[idx] = i.len()
        }
    }
}

fn get_header_col_max_lens(header_row: &[ColDesc], max_len: &mut [usize]) {
    for (idx, i) in header_row.iter().enumerate() {
        if i.get_print_name().len() > max_len[idx] {
            max_len[idx] = i.get_print_name().len()
        }
    }
}
