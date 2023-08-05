use anyhow::Error;

use super::eng::ColDesc;

pub trait ConnectionFn {
    type Cursor<'a>
    where
        Self: 'a;

    fn execute(&self, q: &str, fetch_num: usize) -> Result<(Vec<ColDesc>, Self::Cursor<'_>), Error>;
    fn fetch(&self, c: &mut Self::Cursor<'_>, fetch_num: usize) -> Result<Vec<Vec<String>>, Error>;
}
