# v0.1 Release Checklist

## Pre-Release Validation

- [ ] All CI workflows pass on `main` (GitHub Actions green)
- [ ] No outstanding high/medium priority issues labeled `v0.1`
- [ ] Code coverage meets baseline (if configured)
- [ ] Security scan passes (cargo audit, etc.)
- [ ] License headers present in all source files

## Documentation

- [ ] README.md covers quickstart end-to-end
- [ ] All docs/ artifacts complete and linked from README
- [ ] YAML spec documented and validated (`docs/architecture/yaml-spec-draft.md`)
- [ ] Deployment guide for local/prod (`deploy/docker-compose/`)
- [ ] API docs generated/available (if applicable)

## Demo & Validation

- [ ] Live demo checklist passes ([DEMO-CHECKLIST.md])
- [ ] Quickstart works on clean checkout (Podman, cargo build, run)
- [ ] End-to-end smoke tests pass (`scripts/yaml_contract_smoke.py`)
- [ ] Demo video recorded (optional, but recommended for v0.1)

## Changelog & Tagging

- [ ] Changelog.md updated with v0.1 highlights
- [ ] All PRs since last release linked
- [ ] `git tag v0.1.0`
- [ ] `git push origin v0.1.0`
- [ ] GitHub release draft created with notes/video