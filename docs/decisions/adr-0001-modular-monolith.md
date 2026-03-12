# ADR-0001: Start as a Modular Monolith

## Status
Accepted

## Decision
Astra will begin as a modular monolith rather than a distributed microservice architecture.

## Why
- simpler local development
- easier self-host installation
- less orchestration overhead on the hot path
- better fit for early-stage product iteration
- materially lower operational burden than Airbyte-like designs

## Guardrails
This decision does **not** mean throwing everything into one undifferentiated binary.

Astra must preserve internal boundaries for:
- scheduler
- runtime control
- connector contracts
- metadata/state storage
- secrets
- observability
- worker transport

## Revisit conditions
We revisit this decision if:
- remote worker pools become necessary for scale or network-topology reasons
- multi-tenant cloud execution pressure requires stronger plane separation
- connector isolation needs exceed subprocess boundaries
