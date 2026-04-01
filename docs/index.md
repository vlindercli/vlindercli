# Index

## Systems

| System                  | Responsibility                                              | Crate(s)                                                | Docs                                                                                         |
| ----------------------- | ----------------------------------------------------------- | ------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| **Domain**              | Shared types, traits, wire protocol                         | `vlinder-core`                                          | [DOMAIN_MODEL.md](DOMAIN_MODEL.md)                                                           |
| **Message Queue**       | Route typed messages between workers                        | `vlinder-nats`, `vlinder-core/queue/`                   | [ARCHITECTURE.md](ARCHITECTURE.md), [CONTRACTS.md § MessageQueue](CONTRACTS.md#messagequeue) |
| **DAG Store**           | Append-only Merkle persistence of all side effects          | `vlinder-sql-state`, `vlinder-git-dag`                  | [TIMELINE.md](TIMELINE.md), [CONTRACTS.md § DagStore](CONTRACTS.md#dagstore)                 |
| **Registry**            | Agent/model manifests, deployment state, jobs               | `vlinder-sql-registry`                                  | [DOMAIN_MODEL.md](DOMAIN_MODEL.md), [CONTRACTS.md § Registry](CONTRACTS.md#registry)         |
| **Harness**             | Conversation API — invoke, fork, promote                    | `vlinder-harness`                                       | [DOMAIN_MODEL.md](DOMAIN_MODEL.md), [CONTRACTS.md § Harness](CONTRACTS.md#harness)           |
| **Container Runtime**   | OCI agent lifecycle via Podman                              | `vlinder-podman-runtime`                                | [ARCHITECTURE.md](ARCHITECTURE.md)                                                           |
| **Lambda Runtime**      | AWS Lambda agent execution                                  | `vlinder-nats-lambda-runtime`, `vlinder-lambda-adapter` | [ADR 109](adr/109-lambda-runtime.md)                                                         |
| **Sidecar**             | In-container HTTP bridge, checkpoint loop, service dispatch | `vlinder-podman-sidecar`                                | [ADR 091](adr/091-sidecar-bridge.md), [ADR 105](adr/105-sidecar-http-callback-model.md)      |
| **Provider Server**     | HTTP service gateway inside the sidecar                     | `vlinder-provider-server`                               | [ADR 120](adr/120-provider-plugin-contract.md)                                               |
| **Inference Workers**   | LLM and embedding calls                                     | `vlinder-ollama`, `vlinder-infer-openrouter`            | [ADR 086](adr/086-inference-api-passthrough.md)                                              |
| **Storage Workers**     | Key-value and vector storage                                | `vlinder-sqlite-kv`, `vlinder-sqlite-vec`               | [ADR 036](adr/036-storage-lifecycle.md)                                                      |
| **Model Catalog**       | Resolve model names to inference backends                   | `vlinder-catalog`                                       | [ADR 094](adr/094-model-name-resolution.md)                                                  |
| **Secret Store**        | Named secret storage for agent credentials                  | `vlinder-nats` (`NatsSecretStore`)                      | [ADR 083](adr/083-secret-store.md), [CONTRACTS.md § SecretStore](CONTRACTS.md#secretstore)   |
| **Supervisor / Daemon** | Process lifecycle, health checks, worker spawn              | `vlinderd`                                              | [ARCHITECTURE.md](ARCHITECTURE.md), [ADR 045](adr/045-daemon-supervisor-split.md)            |
| **CLI**                 | User interface                                              | `vlinder`                                               | [ADR 021](adr/021-cli-subcommand-structure.md)                                               |

---

## Key Flows

| Flow                                     | Doc                                                |
| ---------------------------------------- | -------------------------------------------------- |
| Agent deploy + run (end to end)          | [REQUEST_FLOW.md](REQUEST_FLOW.md)                 |
| Session fork, replay, promote            | [TIMELINE_WALKTHROUGH.md](TIMELINE_WALKTHROUGH.md) |
| Write path (CQRS)                        | [ARCHITECTURE.md](ARCHITECTURE.md)                 |
| Agent lifecycle (deploy → live → delete) | [ARCHITECTURE.md](ARCHITECTURE.md)                 |
| Observability and log correlation        | [OBSERVABILITY.md](OBSERVABILITY.md)               |

---

## Module Contracts

Invariants and error guarantees for heavily-used modules: [CONTRACTS.md](CONTRACTS.md)

---

## Known Gaps

Incomplete areas and fragility flags: [KNOWN_GAPS.md](KNOWN_GAPS.md)

---

## Architecture Decision Records

125 ADRs in [`docs/adr/`](adr/). Foundational decisions:

| ADR                                            | Decision                                         |
| ---------------------------------------------- | ------------------------------------------------ |
| [018](adr/018-protocol-first-architecture.md)  | Queue-based, protocol-first architecture         |
| [062](adr/062-remove-inmemory-runtime-mode.md) | No in-process mode — all execution through queue |
| [081](adr/081-time-travel.md)                  | Content-addressed submission chaining            |
| [097](adr/097-content-addressed-identity.md)   | Content-addressed node identity                  |
| [121](adr/121-operational-planes.md)           | Data / Session / Infra plane separation          |
