use wiremock::matchers::{method, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mount a catch-all S3 HEAD response (404 = not found, triggers upload).
pub async fn mock_s3_head_not_found(server: &MockServer) {
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(404))
        .mount(server)
        .await;
}

/// Mount a catch-all S3 PUT response (200 = upload success).
pub async fn mock_s3_put_success(server: &MockServer) {
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .mount(server)
        .await;
}

/// Mount an S3 ListObjectsV2 response that returns empty results.
/// Matches GET requests with `list-type=2` query parameter (S3 ListObjectsV2).
pub async fn mock_s3_list_empty(server: &MockServer) {
    Mock::given(method("GET"))
        .and(query_param("list-type", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bucket</Name>
  <Prefix></Prefix>
  <KeyCount>0</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
</ListBucketResult>"#,
        ))
        .mount(server)
        .await;
}

/// Mount an S3 ListObjectsV2 response that returns the given keys.
pub async fn mock_s3_list_objects(server: &MockServer, keys: &[&str]) {
    let contents: String = keys
        .iter()
        .map(|k| format!(
            "<Contents>\
             <Key>{k}</Key>\
             <LastModified>2024-06-15T10:30:00.000Z</LastModified>\
             <ETag>\"d41d8cd98f00b204e9800998ecf8427e\"</ETag>\
             <Size>100</Size>\
             <StorageClass>STANDARD</StorageClass>\
             </Contents>"
        ))
        .collect();
    Mock::given(method("GET"))
        .and(query_param("list-type", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name>
  <Prefix></Prefix>
  <KeyCount>{}</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  {}
</ListBucketResult>"#,
            keys.len(),
            contents,
        )))
        .mount(server)
        .await;
}

/// Mount an S3 GetObject response for a specific key with the given body.
pub async fn mock_s3_get_object(server: &MockServer, key: &str, body: &[u8]) {
    use wiremock::matchers::path;
    // S3 path-style: /<bucket>/<key>
    // With endpoint override, the SDK uses path-style addressing
    Mock::given(method("GET"))
        .and(path(format!("/{key}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(body.to_vec())
                .insert_header("last-modified", "Thu, 15 Jun 2024 10:30:00 GMT"),
        )
        .mount(server)
        .await;
}

/// Mount an S3 GetObject response with custom metadata headers.
pub async fn mock_s3_get_object_with_metadata(
    server: &MockServer,
    key: &str,
    body: &[u8],
    metadata: &[(&str, &str)],
) {
    use wiremock::matchers::path;
    let mut response = ResponseTemplate::new(200)
        .set_body_bytes(body.to_vec())
        .insert_header("last-modified", "Thu, 15 Jun 2024 10:30:00 GMT");
    for (k, v) in metadata {
        response = response.insert_header(format!("x-amz-meta-{k}"), *v);
    }
    Mock::given(method("GET"))
        .and(path(format!("/{key}")))
        .respond_with(response)
        .mount(server)
        .await;
}

/// Mount a catch-all S3 GetObject response for any GET request.
/// Useful when the exact key path is hard to predict.
pub async fn mock_s3_get_object_catchall(
    server: &MockServer,
    body: &[u8],
    metadata: &[(&str, &str)],
) {
    let mut response = ResponseTemplate::new(200)
        .set_body_bytes(body.to_vec())
        .insert_header("last-modified", "Thu, 15 Jun 2024 10:30:00 GMT");
    for (k, v) in metadata {
        response = response.insert_header(format!("x-amz-meta-{k}"), *v);
    }
    Mock::given(method("GET"))
        .respond_with(response)
        .mount(server)
        .await;
}
