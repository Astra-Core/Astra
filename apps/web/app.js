async function loadPipelines() {
  const el = document.getElementById('pipelinesList');
  try {
    const res = await fetch('/api/v1/pipelines');
    const data = await res.json();
    el.innerHTML = data.pipelines
      .map(
        (p) => `
          <div class="pipeline-row">
            <div>
              <strong>${p.name}</strong>
              <div class="muted">${p.source_kind} → ${p.destination_kind}</div>
            </div>
            <span class="badge">${p.status}</span>
          </div>`
      )
      .join('');
  } catch (err) {
    el.textContent = 'Failed to load pipelines.';
  }
}

async function loadYaml() {
  const el = document.getElementById('yamlPreview');
  try {
    const res = await fetch('/api/v1/examples/postgres-to-warehouse');
    const data = await res.text();
    el.textContent = data;
  } catch (err) {
    el.textContent = 'Failed to load example YAML.';
  }
}

document.getElementById('refreshBtn').addEventListener('click', () => {
  loadPipelines();
  loadYaml();
});

loadPipelines();
loadYaml();
