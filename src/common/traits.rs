use anyhow::Error;

pub trait ConnectionFn {
    type Cursor<'a>
    where
        Self: 'a;

    fn execute(&self, q: &str, fetch_num: usize) -> Result<Self::Cursor<'_>, Error>;
    fn fetch(&self, c: &mut Self::Cursor<'_>, fetch_num: usize) -> Result<Vec<Vec<String>>, Error>;
}
