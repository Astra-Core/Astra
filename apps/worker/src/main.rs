use anyhow::{anyhow, Context};
use clap::Parser;
use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};
use uuid::Uuid;

#[derive(Parser)]
#[command(name = "astra-worker")]
struct Args {
    pipeline_name: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let args = Args::parse();
    info!("astra-worker starting for pipeline: {}", args.pipeline_name);

    let client = Client::new();
    let base_url = "http://localhost:8080";

    // Fetch pipeline spec (optional, but verifies exists)
    let yaml_res = client
        .get(format!(
            "{}/api/v1/pipelines/{}",
            base_url, args.pipeline_name
        ))
        .send()
        .await
        .context("failed to fetch pipeline spec")?;
    if !yaml_res.status().is_success() {
        return Err(anyhow!("pipeline {} not found", args.pipeline_name));
    }
    let yaml = yaml_res.text().await?;
    info!("Fetched pipeline spec: {} chars", yaml.len());

    // Create run
    let create_req = json!({
        "pipeline_name": args.pipeline_name,
        "trigger_mode": "manual"
    });
    let run_resp = client
        .post(format!("{}/api/v1/pipeline-runs", base_url))
        .json(&create_req)
        .send()
        .await
        .context("failed to create pipeline run")?
        .json::<Value>()
        .await
        .context("failed to parse create run response")?;
    let run_id_str: String = run_resp["id"]
        .as_str()
        .context("no run id in response")?
        .to_string();
    let run_id = Uuid::parse_str(&run_id_str).context("invalid run id")?;
    info!("Created run: {}", run_id);

    println!("Running...");

    // Simulate work
    sleep(Duration::from_secs(10)).await;

    // Update status
    let update_req = json!({
        "status": "succeeded",
        "stats_json": {"tables": 2, "rows": 1500}
    });
    let update_res = client
        .post(format!(
            "{}/api/v1/pipeline-runs/{}/status",
            base_url, run_id
        ))
        .json(&update_req)
        .send()
        .await
        .context("failed to update run status")?;
    if !update_res.status().is_success() {
        return Err(anyhow!(
            "status update failed: {}",
            update_res.text().await?
        ));
    }
    info!("Updated run {} to succeeded", run_id);

    println!("Pipeline execution complete.");

    Ok(())
}
