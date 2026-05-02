use wiremock::matchers::{method, body_string_contains};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mount a successful STS `GetCallerIdentity` response.
/// STS uses the query API (POST with form-encoded body), not JSON.
pub async fn mock_get_caller_identity(server: &MockServer) {
    Mock::given(method("POST"))
        .and(body_string_contains("Action=GetCallerIdentity"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <GetCallerIdentityResult>
    <UserId>AIDACKCEVSQ6C2EXAMPLE</UserId>
    <Account>123456789012</Account>
    <Arn>arn:aws:iam::123456789012:user/test</Arn>
  </GetCallerIdentityResult>
</GetCallerIdentityResponse>"#,
        ))
        .mount(server)
        .await;
}

/// Mount a failing STS `GetCallerIdentity` response (expired/invalid creds).
pub async fn mock_get_caller_identity_failure(server: &MockServer) {
    Mock::given(method("POST"))
        .and(body_string_contains("Action=GetCallerIdentity"))
        .respond_with(ResponseTemplate::new(403).set_body_string(
            r#"<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <Error>
    <Type>Sender</Type>
    <Code>ExpiredTokenException</Code>
    <Message>The security token included in the request is expired</Message>
  </Error>
</ErrorResponse>"#,
        ))
        .mount(server)
        .await;
}
