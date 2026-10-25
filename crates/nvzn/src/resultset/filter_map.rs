use crate::ResultSet;

impl<'a, T: 'a> ResultSet<'a, T> {
    pub fn filter_map<S: 'a>(self, mut f: impl FnMut(T) -> Option<S> + 'a) -> ResultSet<'a, S> {
        self.with_rows(|rows| Box::new(rows.filter_map(move |r| r.map(&mut f).transpose())))
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use crate::{
        resultset::test_support::{rig, seed},
        testsupport::{Todo, UserId},
    };

    #[tokio::test]
    async fn filter_map_drops_none_rows() {
        let (store, reader, _idx) = rig().await;
        seed(&store);

        let mut filtered = reader
            .snapshot()
            .unwrap()
            .select::<Todo>()
            .filter_map(|t: Todo| (t.user_id.0 == 100).then_some(t.title))
            .collect()
            .unwrap();
        filtered.sort();
        assert_eq!(filtered, vec!["one", "two"]);
    }

    #[tokio::test]
    async fn filter_map_can_capture() {
        let (store, reader, _idx) = rig().await;
        seed(&store);

        let wanted = UserId(100);
        let mut filtered = reader
            .snapshot()
            .unwrap()
            .select::<Todo>()
            .filter_map(move |t: Todo| (t.user_id == wanted).then_some(t.title))
            .collect()
            .unwrap();
        filtered.sort();
        assert_eq!(filtered, vec!["one", "two"]);
    }
}
