use crate::ResultSet;

impl<'a, T: 'a> ResultSet<'a, T> {
    pub fn filter_map<S: 'a>(self, mut f: impl FnMut(T) -> Option<S> + 'a) -> ResultSet<'a, S> {
        self.with_rows(|rows| Box::new(rows.filter_map(move |r| r.map(&mut f).transpose())))
    }
}
