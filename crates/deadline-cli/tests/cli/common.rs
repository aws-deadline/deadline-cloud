use deadline_test_server::TestHarness;
use insta_cmd::assert_cmd_snapshot;

#[tokio::test]
async fn version_prints_name_and_semver() {
    let harness = TestHarness::new().await;

    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r"deadline \d+\.\d+\.\d+", "deadline [VERSION]");
    let _guard = settings.bind_to_scope();

    assert_cmd_snapshot!(harness.cmd(&["--version"]));
}

#[tokio::test]
async fn help_long_flag_prints_usage() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["--help"]));
}

#[tokio::test]
async fn help_short_flag_prints_usage() {
    let harness = TestHarness::new().await;
    assert_cmd_snapshot!(harness.cmd(&["-h"]));
}
