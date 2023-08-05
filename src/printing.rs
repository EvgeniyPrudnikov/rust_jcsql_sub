use std::io::{self, Result, Write};

#[derive(Debug)]
pub struct CellSize {
    pub width: usize,
    _height: usize,
    pub is_left: bool,
    pub is_right: bool,
    pub is_top: bool,
    pub is_bot: bool,
}

impl CellSize {
    pub fn new(width: usize, is_left: bool, is_right: bool, is_top: bool, is_bot: bool) -> Self {
        Self {
            width: width + CellSize::get_base_width(),
            _height: 1,
            is_left,
            is_right,
            is_top,
            is_bot,
        }
    }
    pub fn get_base_width() -> usize {
        4
    }
}

impl Default for CellSize {
    fn default() -> Self {
        Self::new(0, true, true, true, true)
    }
}

#[derive(Debug)]
pub struct CellParams {
    pub value: String,
    pub value_bytes_cnt: usize,
    pub value_chars_cnt: usize,
    pub sizes: CellSize,
}

impl CellParams {
    pub fn new(
        value: String,
        value_bytes_cnt: usize,
        value_chars_cnt: usize,
        sizes: CellSize,
    ) -> Self {
        Self {
            value,
            value_bytes_cnt,
            value_chars_cnt,
            sizes,
        }
    }
}

#[derive(Debug)]
pub struct CellLines {
    pub l_top: String,
    pub l_val: String,
    pub l_other_val: Vec<String>,
    pub l_bot: String,
}

impl Default for CellLines {
    fn default() -> Self {
        Self::new("".to_owned(), "".to_owned(), Vec::new(), "".to_owned())
    }
}

impl CellLines {
    pub fn new(l_top: String, l_val: String, l_other_val: Vec<String>, l_bot: String) -> Self {
        CellLines {
            l_top,
            l_val,
            l_other_val,
            l_bot,
        }
    }
    pub fn print(&self) -> Result<()> {
        let new_line_bytes = "\n".as_bytes();
        if !self.l_top.is_empty() {
            io::stdout().write_all(self.l_top.as_bytes())?;
            io::stdout().write_all(new_line_bytes)?;
        }

        io::stdout().write_all(self.l_val.as_bytes())?;
        io::stdout().write_all(new_line_bytes)?;

        if !self.l_bot.is_empty() {
            io::stdout().write_all(self.l_bot.as_bytes())?;
            io::stdout().write_all(new_line_bytes)?;
        }
        Ok(())
    }
}

pub fn cell_to_print(cp: CellParams) -> CellLines {
    let mut l_top = {
        if cp.sizes.is_top {
            cp.sizes.width
        } else {
            0
        }
    };
    let mut l_val = cp.sizes.width;

    let mut l_bot = {
        if cp.sizes.is_bot {
            cp.sizes.width
        } else {
            0
        }
    };

    let mut to_print_line: CellLines = CellLines::default();

    if l_top > 0 {
        if cp.sizes.is_left {
            to_print_line.l_top += "+"
        };
        // print for left
        l_top -= 1;
        // for right also
        l_top -= 1;

        for _n in 0..l_top {
            to_print_line.l_top += "-";
        }

        if cp.sizes.is_right {
            to_print_line.l_top += "+";
        }
    }
    {
        if cp.sizes.is_left {
            to_print_line.l_val += "|"
        };
        l_val -= 1;
        l_val -= 1;

        to_print_line.l_val += " ";
        l_val -= 1;

        to_print_line.l_val += cp.value.as_str();
        l_val -= cp.value_chars_cnt;

        for _n in 0..l_val {
            to_print_line.l_val += " ";
        }

        if cp.sizes.is_right {
            to_print_line.l_val += "|";
        }
    }

    if l_bot > 0 {
        if cp.sizes.is_left {
            to_print_line.l_bot += "+"
        };
        l_bot -= 1;
        l_bot -= 1;

        for _n in 0..l_bot {
            to_print_line.l_bot += "-";
        }

        if cp.sizes.is_right {
            to_print_line.l_bot += "+";
        }
    }

    to_print_line
}

pub fn print_cells_line(lines: Vec<CellLines>) {
    let mut res_line = CellLines::default();
    for i in lines {
        res_line.l_top += i.l_top.as_str();
        res_line.l_val += i.l_val.as_str();
        res_line.l_bot += i.l_bot.as_str();
    }
    res_line.print().unwrap()
}
