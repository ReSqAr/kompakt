//! The S3 form of `ZSLG_OBJECT_STORE`: one variable, key pair included.
//!
//! ```text
//!   s3://bucket[/prefix]?endpoint=URL&region=R&access_key=K&secret_key=S
//!        │       └ keys below it       └ region defaults to us-east-1
//!        └ the log's bucket
//! ```
//!
//! The query is split on `&` and then on the first `=`; values are literal
//! (`/`, `+` and a trailing `=` are fine in a secret). A secret holding `&` or
//! `#` cannot be written; the error says what it needs. The value is a secret:
//! no message here repeats it, and the spec never prints the key pair.

use super::ConfigError;
use zslg::ObjectStoreConfig;

const EXPECTED: &str = "s3://bucket[/prefix]?endpoint=URL&region=R&access_key=K&secret_key=S";

/// The backend and the key prefix below which this store lives.
pub(super) fn s3(var: &'static str, raw: &str) -> Result<(ObjectStoreConfig, String), ConfigError> {
    // Never echo the value: it carries the secret key.
    let bad = |why: &str| ConfigError::bad_value(var, "(hidden)", format!("{EXPECTED} — {why}"));
    let rest = raw
        .trim()
        .strip_prefix("s3://")
        .ok_or_else(|| bad("not an s3:// location"))?;
    let (path, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (bucket, prefix) = path.split_once('/').unwrap_or((path, ""));
    if bucket.is_empty() {
        return Err(bad("no bucket"));
    }
    let param = |name: &str| -> Option<String> {
        query
            .split('&')
            .filter_map(|kv| kv.split_once('='))
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.to_owned())
    };
    let endpoint = param("endpoint").ok_or_else(|| bad("no endpoint="))?;
    let access_key = param("access_key")
        .filter(|k| !k.is_empty())
        .ok_or_else(|| bad("no access_key="))?;
    let secret_key = param("secret_key")
        .filter(|k| !k.is_empty())
        .ok_or_else(|| bad("no secret_key= (a secret holding & or # cannot be written here)"))?;
    let region = param("region").unwrap_or_else(|| "us-east-1".to_owned());
    Ok((
        ObjectStoreConfig::S3 {
            allow_http: endpoint.starts_with("http://"),
            endpoint,
            bucket: bucket.to_owned(),
            access_key,
            secret_key,
            region,
        },
        prefix.trim_matches('/').to_owned(),
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, reason = "test code")]
mod tests {
    use super::*;

    #[test]
    fn the_key_pair_is_read_from_the_query_literally() {
        let (config, prefix) = s3(
            "ZSLG_OBJECT_STORE",
            "s3://bucket/bucket/zslg?endpoint=http://s3:9000&region=nbg1&access_key=AK/1+x&secret_key=s3/cr+t==",
        )
        .unwrap();
        assert_eq!(prefix, "bucket/zslg");
        match config {
            ObjectStoreConfig::S3 {
                endpoint,
                bucket,
                access_key,
                secret_key,
                region,
                allow_http,
            } => {
                assert_eq!(endpoint, "http://s3:9000");
                assert_eq!(bucket, "bucket");
                assert_eq!(access_key, "AK/1+x");
                assert_eq!(secret_key, "s3/cr+t==");
                assert_eq!(region, "nbg1");
                assert!(allow_http);
            }
            other => panic!("expected S3, got {other:?}"),
        }
    }

    #[test]
    fn a_refusal_never_repeats_the_secret() {
        for raw in [
            "s3://bucket?endpoint=https://s3&access_key=AK",
            "s3://bucket?access_key=AK&secret_key=TOPSECRET",
            "s3://?endpoint=https://s3&access_key=AK&secret_key=TOPSECRET",
        ] {
            let message = s3("ZSLG_OBJECT_STORE", raw).unwrap_err().to_string();
            assert!(message.contains("ZSLG_OBJECT_STORE"), "{message}");
            assert!(!message.contains("TOPSECRET"), "{message}");
            assert!(!message.contains("AK"), "{message}");
        }
    }

    #[test]
    fn the_spec_never_prints_its_secret() {
        let (config, _) = s3(
            "ZSLG_OBJECT_STORE",
            "s3://b?endpoint=https://s3&access_key=AK&secret_key=TOPSECRET",
        )
        .unwrap();
        assert!(!format!("{config:?}").contains("TOPSECRET"));
    }
}
