# ADR 129: S3 + S3 Vectors Storage PoC

**Status:** Draft

## Context

Phase 2 of the three-step PoC defined in ADR 127. The storage trait contract this PoC will exercise is the one proposed in ADRs 097–100 (still drafts).

## What S3 and S3 Vectors are

**S3** — object storage with native versioning. Bucket versioning makes every `PutObject` return an immutable `versionId`; old versions are retained. Multi-cloud (S3, GCS, Azure Blob, MinIO).

**S3 Vectors** — managed vector database built into S3. Native API: `PutVectors`, `GetVectors`, `QueryVectors`, `DeleteVectors`. Filterable metadata on every vector. AWS-only.

## Goal

Evaluate S3 (with native versioning) and S3 Vectors as production storage for Vlinder's object and vector concerns.

## Status

Not started. Depends on learnings from ADR 128.
