# Astra Metadata Schema Draft

Core entities for v0.1:
- sources
- destinations
- pipelines
- pipeline_specs
- jobs
- job_runs
- checkpoints
- schema_events
- secrets_refs

## Lifecycle notes
- pipelines reference the latest active spec version
- jobs represent scheduled or operator-triggered work
- job_runs are immutable executions
- checkpoints are scoped to pipeline + stream/table + phase
- schema_events record additive vs breaking changes

## Open questions
- whether checkpoints should be split between snapshot and CDC tables
- whether sink commit markers live beside checkpoints or in a dedicated apply ledger
