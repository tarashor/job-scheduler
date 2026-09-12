package com.tarashor.scheduler.storage

/**
 * Composite Storage interface combining all domain storage ports.
 * Useful for development, testing, and backward-compatible unified storage instances.
 */
interface SchedulerStorage : JobMetadataStore, RunHistoryStore, WorkerRegistry, OutboxStore
