use anyhow::{anyhow, Context};
use clap::Parser;
use reqwest::Client;
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::info;
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

    let start_time = Instant::now();
    let stages = [
        ("Planning", Duration::from_secs(1)),
        ("Snapshot", Duration::from_secs(3)),
        ("StageFlush", Duration::from_secs(2)),
        ("SinkApply", Duration::from_secs(3)),
        ("Finalize", Duration::from_secs(1)),
    ];

    for (idx, (phase, duration)) in stages.iter().enumerate() {
        info!("Starting stage: {}", phase);
        sleep(*duration).await;

        let progress = json!({"current": (idx + 1) as u32, "total": 5u32});
        let status_req = json!({
            "status": "running",
            "phase": phase,
            "progress": progress
        });
        let update_res = client
            .post(format!(
                "{}/api/v1/pipeline-runs/{}/status",
                base_url, run_id
            ))
            .json(&status_req)
            .send()
            .await
            .context(format!("failed to update status for stage {}", phase))?;
        if !update_res.status().is_success() {
            return Err(anyhow!(
                "stage {} update failed: {}",
                phase,
                update_res.text().await?
            ));
        }
        info!("Completed stage {} ({} / 5)", phase, idx + 1);
    }

    let duration_ms = start_time.elapsed().as_millis() as u64;
    let final_stats = json!({
        "duration_ms": duration_ms,
        "tables": 2,
        "errors": []
    });
    let final_req = json!({
        "status": "succeeded",
        "stats_json": final_stats
    });
    let final_res = client
        .post(format!(
            "{}/api/v1/pipeline-runs/{}/status",
            base_url, run_id
        ))
        .json(&final_req)
        .send()
        .await
        .context("failed to send final status")?;
    if !final_res.status().is_success() {
        return Err(anyhow!(
            "final status update failed: {}",
            final_res.text().await?
        ));
    }
    info!("Updated run {} to succeeded with stats", run_id);

    println!("Pipeline execution complete.");

    Ok(())
}
