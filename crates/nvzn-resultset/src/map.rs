use crate::ResultSet;
use nvzn_types::Error;

impl<'a, T: 'a> ResultSet<'a, T> {
    pub fn map<S: 'a>(self, mut f: impl FnMut(T) -> S + 'a) -> ResultSet<'a, S> {
        self.try_map(move |t| Ok(f(t)))
    }

    pub fn try_map<S: 'a>(self, mut f: impl FnMut(T) -> Result<S, Error> + 'a) -> ResultSet<'a, S> {
        self.with_rows(|rows| Box::new(rows.map(move |r| r.and_then(&mut f))))
    }
}
