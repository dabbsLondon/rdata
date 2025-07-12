use assert_cmd::Command;

#[test]
fn runs() {
    let mut cmd = Command::cargo_bin("polars-query-server").unwrap();
    cmd.env("SKIP_SERVER", "1");
    cmd.assert().success();
}
