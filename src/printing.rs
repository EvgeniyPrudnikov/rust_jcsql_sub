use std::io::{self, Result, Write};

// const NEW_LINE_BYTES: &[u8] = &[10];

#[derive(Debug)]
pub struct ColDesc {
    pub col_id: usize,
    pub col_name: String,
    pub col_type: String,
}

impl ColDesc {
    pub fn new(col_id: usize, col_name: String, col_type: String) -> Self {
        ColDesc {
            col_id,
            col_name,
            col_type,
        }
    }
    pub fn get_print_name(&self) -> String {
        format!("{}({})", self.col_name, self.col_type)
    }
}
#[derive(Debug)]
pub struct CellSize {
    pub width: usize,
    height: usize,
    pub is_left: bool,
    pub is_right: bool,
    pub is_top: bool,
    pub is_bot: bool,
}

impl CellSize {
    pub fn new(width: usize, is_left: bool, is_right: bool, is_top: bool, is_bot: bool) -> Self {
        Self {
            width: width + CellSize::get_base_width(),
            height: 1,
            is_left,
            is_right,
            is_top,
            is_bot,
        }
    }
    fn get_base_width() -> usize {
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
    pub sizes: CellSize,
}

impl CellParams {
    pub fn new(value: String, sizes: CellSize) -> Self {
        Self { value, sizes }
    }
}
#[derive(Debug)]
pub struct Line {
    pub l_top: String,
    pub l_val: String,
    pub l_other_val: Vec<String>,
    pub l_bot: String,
}

impl Default for Line {
    fn default() -> Self {
        Self::new("".to_owned(), "".to_owned(), Vec::new(), "".to_owned())
    }
}

impl Line {
    pub fn new(l_top: String, l_val: String, l_other_val: Vec<String>, l_bot: String) -> Self {
        Line {
            l_top,
            l_val,
            l_other_val,
            l_bot,
        }
    }
    pub fn print(&self) -> Result<()> {
        let new_line_bytes = "\n".as_bytes();
        io::stdout().write_all(self.l_top.as_bytes())?;
        io::stdout().write_all(new_line_bytes)?;
        io::stdout().write_all(self.l_val.as_bytes())?;
        io::stdout().write_all(new_line_bytes)?;
        io::stdout().write_all(self.l_bot.as_bytes())?;
        io::stdout().write_all(new_line_bytes)?;
        Ok(())
    }
}
