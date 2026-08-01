use anyhow::Result;
use app_test_support::TestAppServer;
use codex_app_server_protocol::JSONRPCError;
use codex_app_server_protocol::RequestId;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::timeout;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const CONTROLLER_NOT_IMPLEMENTED: &str = "external controller APIs are not implemented yet";

#[tokio::test]
async fn controller_methods_return_not_implemented_stub() -> Result<()> {
    let codex_home = TempDir::new()?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build()
        .await?;
    mcp.initialize().await?;

    for (method, params) in [
        (
            "controller/requestParticipation",
            Some(json!({
                "controllerName": "codex-waveshare",
                "description": "Codex Waveshare controller"
            })),
        ),
        ("controller/acquireControl", None),
        ("controller/releaseControl", None),
        ("controller/signOff", None),
    ] {
        let request_id = mcp.send_raw_request(method, params).await?;
        let error = timeout(
            DEFAULT_TIMEOUT,
            mcp.read_stream_until_error_message(RequestId::Integer(request_id)),
        )
        .await??;

        assert_controller_not_implemented_error(error);
    }

    Ok(())
}

fn assert_controller_not_implemented_error(error: JSONRPCError) {
    assert_eq!(error.error.code, -32601);
    assert_eq!(error.error.message, CONTROLLER_NOT_IMPLEMENTED);
    assert_eq!(error.error.data, None);
}
