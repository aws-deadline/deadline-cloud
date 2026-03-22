use serde_json::{Value, json};
use wiremock::matchers::{header, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mount a ListFarms response returning the given farms.
///
/// Each farm should be a JSON object with at least `farmId` and `displayName`.
pub async fn mock_list_farms(server: &MockServer, farms: &[Value]) {
    Mock::given(method("POST"))
        .and(header("x-amz-target", "Deadline.ListFarms"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({
                "farms": farms,
                "nextToken": null
            })),
        )
        .mount(server)
        .await;
}

/// Mount a GetFarm response for a specific farm.
pub async fn mock_get_farm(server: &MockServer, farm: Value) {
    Mock::given(method("POST"))
        .and(header("x-amz-target", "Deadline.GetFarm"))
        .respond_with(ResponseTemplate::new(200).set_body_json(farm))
        .mount(server)
        .await;
}
