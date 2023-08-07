use anyhow::Error;
use common::{eng::ColDesc, ConnectionFn};

mod common;
mod printing;
use common::args::Args;
mod engines;
use engines::impala::Impala;

use crate::printing::{CellLines, CellParams, CellSize};
use chrono::{Duration, Local};
// use std::collections::VecDeque;

fn format_duration(duration: Duration) -> String {
    let hours = duration.num_hours();
    let minutes = duration.num_minutes() % 60;
    let seconds = duration.num_seconds() % 60;
    let milliseconds = duration.num_milliseconds() % 1000;

    format!(
        "{:02}:{:02}:{:02}:{:03}",
        hours, minutes, seconds, milliseconds
    )
}

fn main() -> Result<(), Error> {
    let a = Args::parse();
    let mut start_msg: Vec<String> = Vec::new();
    let mut end_msg: Vec<String> = Vec::new();

    let client = Impala::new(a.connection_string.clone());
    start_msg.push(format!(
        "[{}] Connected to {:?}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        client.engine
    ));

    let raw_query = a.get_query();
    let queries = split_queries(raw_query);
    let queries_cnt = queries.len();

    for query in queries {
        if query.is_empty() {
            panic!("Empty query!");
        }

        start_msg.push(query.clone());

        let start_time = chrono::Local::now();
        let (col_desc, mut c) = client.execute(&query, a.fetch_num)?;
        let data = client.fetch(&mut c, a.fetch_num)?;
        let duration = chrono::Local::now() - start_time;
        end_msg.push(format!("Elapsed {} s", format_duration(duration)));

        //------ process data ----------------
        let print_buffer = to_print_buffer(&col_desc, &data);
        //------ print result ----------------
        print_message(&start_msg, print_buffer, &end_msg);
        if queries_cnt > 1 {
            start_msg.pop();
            end_msg.pop();
        }
    }

    Ok(())
}

fn upd_data_col_max_lens(data_row: &[String], max_len: &mut [usize]) {
    for (idx, i) in data_row.iter().enumerate() {
        let chars_cnt = i.chars().count();
        if chars_cnt > max_len[idx] {
            max_len[idx] = chars_cnt
        }
    }
}

fn upd_header_col_max_lens(header_row: &[ColDesc], max_len: &mut [usize]) {
    for (idx, i) in header_row.iter().enumerate() {
        let chars_cnt = i.get_print_name().chars().count();
        if chars_cnt > max_len[idx] {
            max_len[idx] = chars_cnt
        }
    }
}

fn print_message(
    start_msg: &Vec<String>,
    print_buffer: Vec<Vec<CellLines>>,
    end_msg: &Vec<String>,
) {
    for smsg in start_msg {
        println!("{}\n", smsg);
    }

    let fetched = print_buffer.len();
    for i in print_buffer {
        printing::print_cells_line(i);
    }
    println!("\nFetched {} rows", fetched - 1);

    for emsg in end_msg {
        println!("{}\n", emsg);
    }
}

fn to_print_buffer(header: &Vec<ColDesc>, data: &Vec<Vec<String>>) -> Vec<Vec<CellLines>> {
    let mut print_buffer: Vec<Vec<CellLines>> = Vec::new();
    let mut col_max: Vec<usize> = vec![0; header.len()];

    upd_header_col_max_lens(header, &mut col_max);
    for i in data.iter() {
        upd_data_col_max_lens(i, &mut col_max);
    }

    let mut print_row: Vec<CellLines> = Vec::new();
    for (col_idx, cd) in header.iter().enumerate() {
        print_row.push(printing::cell_to_print(CellParams::new(
            cd.get_print_name(),
            cd.get_print_name().len(),
            cd.get_print_name().chars().count(),
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
                col.len(),
                col.chars().count(),
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
    print_buffer
}

fn split_queries(queries: String) -> Vec<String> {
    let mut res: Vec<String> = Vec::new();
    let mut quote_started = false;

    let mut pos: Vec<usize> = Vec::new();

    for (i, ch) in queries.chars().enumerate() {
        if ch == '\'' && !quote_started {
            quote_started = true;
            continue;
        }
        if ch == ';' && !quote_started {
            pos.push(i)
        }

        if ch == '\'' && (quote_started || ch == '\n') {
            quote_started = false;
            continue;
        }
    }

    let mut start_index = 0;
    let mut end_index;
    for m in pos {
        end_index = m;
        let substring = queries.get(start_index..end_index).unwrap_or("").trim();
        res.push(substring.to_owned());
        start_index = end_index + 1;
    }
    // rest of string
    let substring = queries.get(start_index..).unwrap_or("").trim();
    if !substring.is_empty() {
        res.push(substring.to_string());
    }

    res
}
