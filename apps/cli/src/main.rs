#[tokio::main]
async fn main() -> anyhow::Result<()> {
    astra::run().await
}
