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
