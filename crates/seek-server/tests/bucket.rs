//! The artifact through an S3-compatible bucket: the builder's upload and `latest.json`, a pod's
//! poll and download, pruning. Needs a bucket: `just seek-minio-test` runs a throwaway MinIO and
//! sets SEEK_TEST_S3 (endpoint), SEEK_TEST_S3_BUCKET and SEEK_TEST_S3_CREDENTIALS.

use seek_server::store::{Bucket, Latest, Source};

#[tokio::test]
#[ignore = "needs an S3-compatible bucket: just seek-minio-test"]
async fn publish_poll_and_prune() {
    let (Ok(ep), Ok(bucket), Ok(creds)) = (
        std::env::var("SEEK_TEST_S3"),
        std::env::var("SEEK_TEST_S3_BUCKET"),
        std::env::var("SEEK_TEST_S3_CREDENTIALS"),
    ) else {
        panic!("SEEK_TEST_S3, SEEK_TEST_S3_BUCKET and SEEK_TEST_S3_CREDENTIALS");
    };
    let url = format!("s3://{bucket}/seek-test-{}", std::process::id());
    let b = Bucket::open(&url, Some(&ep), "us-east-1", Some(std::path::Path::new(&creds))).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut versions = Vec::new();
    for i in 0..3u64 {
        let v = format!("20261114T00000{i}Z000-{i}");
        let p = dir.path().join(format!("{v}.seek"));
        // bigger than one 32 MiB part, so the upload is multipart
        let body: Vec<u8> = (0..(40u64 << 20) + i).map(|x| (x * 31 + i) as u8).collect();
        std::fs::write(&p, &body).unwrap();
        let l = Latest {
            version: v.clone(),
            key: format!("{v}.seek"),
            bytes: body.len() as u64,
            cursor_us: i,
            created_ms: i as i64,
        };
        b.publish(&p, &l).await.unwrap();
        versions.push((v, body));
    }
    b.prune(2).await.unwrap();
    let latest = b.latest().await.unwrap().unwrap();
    assert_eq!(latest.version, versions[2].0);

    let pod = tempfile::tempdir().unwrap();
    let src = Source::parse(&url, Some(&ep), "us-east-1", Some(std::path::Path::new(&creds))).unwrap();
    let f = src.poll(None, pod.path()).await.unwrap().unwrap();
    assert_eq!(f.version, versions[2].0);
    assert_eq!(std::fs::read(&f.path).unwrap(), versions[2].1);
    assert!(src.poll(Some(&versions[2].0), pod.path()).await.unwrap().is_none());
}
