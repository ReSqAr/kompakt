use crate::{Error, ResultSet};

impl<'a, T: 'a> ResultSet<'a, T> {
    pub fn map<S: 'a>(self, mut f: impl FnMut(T) -> S + 'a) -> ResultSet<'a, S> {
        self.try_map(move |t| Ok(f(t)))
    }

    pub fn try_map<S: 'a>(self, mut f: impl FnMut(T) -> Result<S, Error> + 'a) -> ResultSet<'a, S> {
        self.with_rows(|rows| Box::new(rows.map(move |r| r.and_then(&mut f))))
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
        Error,
        resultset::test_support::{rig, seed},
        testsupport::Todo,
    };

    #[tokio::test]
    async fn map_transforms_rows() {
        let (store, reader, _idx) = rig().await;
        seed(&store);

        let mut mapped = reader
            .snapshot()
            .unwrap()
            .select::<Todo>()
            .map(|t: Todo| t.title)
            .collect()
            .unwrap();
        mapped.sort();
        assert_eq!(mapped, vec!["one", "three", "two"]);
    }

    #[tokio::test]
    async fn map_can_capture() {
        let (store, reader, _idx) = rig().await;
        seed(&store);

        let prefix = String::from("todo:");
        let mut labelled = reader
            .snapshot()
            .unwrap()
            .select::<Todo>()
            .map(move |t: Todo| format!("{prefix}{}", t.title))
            .collect()
            .unwrap();
        labelled.sort();
        assert_eq!(labelled, vec!["todo:one", "todo:three", "todo:two"]);
    }

    #[tokio::test]
    async fn try_map_surfaces_the_first_error() {
        let (store, reader, _idx) = rig().await;
        seed(&store);

        let err = reader
            .snapshot()
            .unwrap()
            .select::<Todo>()
            .try_map(|t: Todo| match t.title.as_str() {
                "two" => Err(Error::Poison("two".to_string())),
                _ => Ok(t.title),
            })
            .collect()
            .unwrap_err();
        assert!(matches!(err, Error::Poison(ref m) if m == "two"), "{err:?}");
    }
}
