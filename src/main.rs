use std::collections::VecDeque;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use anyhow::Error;
use common::{eng::ColDesc, ConnectionFn};

mod common;
mod printing;
use common::args::Args;
mod engines;
use engines::impala::Impala;

use crate::printing::{CellLines, CellParams, CellSize};
use chrono::{Duration, Local};

const PRINT_LOAD: &str = "(...)";
const FETCHED_ALL_ROWS: &str = "Fetched all rows.";

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

    let client = match a.engine {
        common::Engines::Impala => Impala::new(a.connection_string.clone()),
        _ => {
            println!("Not Implemented");
            std::process::exit(1)
        }
    };

    start_msg.push(format!(
        "[{}] Connected to {:?}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        client.engine
    ));

    let raw_query = a.get_query();
    let queries = split_queries(raw_query);
    let queries_cnt = queries.len();
    let mut is_fetched_all_rows = false;
    let mut result_buffer = Vec::new();
    let mut columns_description = Vec::new();
    let mut cursor = None;

    for query in queries {
        if query.is_empty() {
            panic!("Empty query!");
        }

        start_msg.push(query.clone());

        let start_time = Local::now();
        match client.execute(&query, a.fetch_num) {
            Ok((col_desc, c)) => {
                cursor = c;
                columns_description = col_desc;
            }
            Err(e) => {
                // println!("{}", e);
                start_msg.push(e.to_string());
                print_message(&start_msg, None, &end_msg);
                std::process::exit(1);
            }
        };
        let (data, fetched_all_rows) = client.fetch(cursor.as_mut().unwrap(), a.fetch_num)?;
        is_fetched_all_rows = fetched_all_rows;
        result_buffer = data;

        let duration = Local::now() - start_time;
        end_msg.push(format!("Elapsed {} s", format_duration(duration)));

        //------ process data ----------------
        let print_buffer = to_print_buffer(&columns_description, &result_buffer);
        //------ print result ----------------
        print_message(&start_msg, Some(print_buffer), &end_msg);
        if queries_cnt > 1 {
            start_msg.pop();
            end_msg.pop();
        }
    }

    if !is_fetched_all_rows {
        println!("{}", PRINT_LOAD);
        end_msg.push(PRINT_LOAD.to_string());
    } else {
        println!("{}", FETCHED_ALL_ROWS);
    }

    let input_deque: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::new()));
    // Clone the Deque for the separate thread to use
    let deque_clone = Arc::clone(&input_deque);
    // Create a channel to communicate between main thread and separate thread
    let (sender, receiver) = mpsc::channel();
    // Spawn a separate thread to listen for user input
    thread::spawn(move || {
        loop {
            let mut input = String::new();
            match std::io::stdin().read_line(&mut input) {
                Ok(_) => {
                    deque_clone
                        .lock()
                        .unwrap()
                        .push_back(input.trim().to_string());
                    sender.send(()).unwrap(); // Notify the main thread
                }
                Err(error) => println!("Error: {}", error),
            }
        }
    });

    // default timeout 30 sec
    let mut timeout = Duration::seconds(5);
    loop {
        // Wait for notification from the separate thread or timeout
        if receiver.recv_timeout(timeout.to_std().unwrap()).is_err() {
            print!("done");
            return Ok(());
        } else {
            timeout = timeout + Duration::seconds(5);

            // Process the input from the Deque in the main thread
            while let Some(input) = input_deque.lock().unwrap().pop_front() {
                let mut parts = input.split("==").collect::<Vec<&str>>();
                let fetch_num = parts.pop().unwrap().parse::<i32>()?;
                let cmd = parts.pop().unwrap().to_string();

                if cmd == "load" {
                    println!("is_fetched_all_rows = {}", is_fetched_all_rows);
                    if !is_fetched_all_rows {
                        let (mut data, fetched_all_rows) =
                            client.fetch(cursor.as_mut().unwrap(), fetch_num)?;
                        is_fetched_all_rows = fetched_all_rows;
                        result_buffer.append(&mut data);

                        if is_fetched_all_rows {
                            if let Some(last_element) = end_msg.last_mut() {
                                *last_element = FETCHED_ALL_ROWS.to_string(); // Update the value of the last element
                            }
                        }
                    } else if let Some(last_element) = end_msg.last_mut() {
                        *last_element = FETCHED_ALL_ROWS.to_string(); // Update the value of the last element
                    }

                    let print_buffer = to_print_buffer(&columns_description, &result_buffer);
                    print_message(&start_msg, Some(print_buffer), &end_msg);
                } else if cmd == "csv" {
                    println!("LOL")
                } else {
                    break;
                }
                /*
                elif cmd[0] == 'csv':
                    if not is_fetched_all_rows:
                        is_fetched_all_rows = fetch_data(cur, output, int(cmd[1]), is_fetched_all_rows)
                    cvs_print_result(output)
                    # break
                else:
                    break

                     */
            }
        }
    }
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
    print_buffer: Option<Vec<Vec<CellLines>>>,
    end_msg: &Vec<String>,
) {
    for smsg in start_msg {
        println!("{}\n", smsg);
    }

    if let Some(print_buffer) = print_buffer {
        let fetched = print_buffer.len();
        for i in print_buffer {
            printing::print_cells_line(i);
        }
        println!("\nFetched {} rows", fetched - 1);
    }

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
