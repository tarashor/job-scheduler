package com.tarashor.scheduler.storage

/**
 * Composite implementation of SchedulerStorage that delegates each domain concern
 * to its dedicated microservice store.
 */
class CompositeSchedulerStorage(
    val jobMetadataStore: JobMetadataStore,
    val runHistoryStore: RunHistoryStore,
    val workerRegistry: WorkerRegistry,
    val outboxStore: OutboxStore = InMemoryOutboxStore(),
    val queueStore: QueueStore = InMemoryQueueStore(),
    val taskStore: TaskStore = InMemoryTaskStore()
) : SchedulerStorage,
    JobMetadataStore by jobMetadataStore,
    RunHistoryStore by runHistoryStore,
    WorkerRegistry by workerRegistry,
    OutboxStore by outboxStore,
    QueueStore by queueStore,
    TaskStore by taskStore

