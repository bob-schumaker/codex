use anyhow::Result;
use app_test_support::TestAppServer;
use codex_app_server_protocol::ControllerErrorCode;
use codex_app_server_protocol::ControllerErrorData;
use codex_app_server_protocol::JSONRPCError;
use codex_app_server_protocol::RequestId;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::timeout;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn controller_methods_require_external_controller_origin() -> Result<()> {
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

        assert_controller_not_allowed_error(error)?;
    }

    Ok(())
}

fn assert_controller_not_allowed_error(error: JSONRPCError) -> Result<()> {
    assert_eq!(error.error.code, -32600);
    assert_eq!(
        error.error.message,
        "controller methods require an external controller connection"
    );
    let data: ControllerErrorData = serde_json::from_value(
        error
            .error
            .data
            .expect("controller error should include data"),
    )?;
    assert_eq!(data.code, ControllerErrorCode::ControllerNotAllowed);
    Ok(())
}
