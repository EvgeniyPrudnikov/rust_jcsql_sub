#[derive(Debug, Clone, Copy)]
pub enum Engines {
    Impala,
    Oracle,
    Snowflake,
}


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