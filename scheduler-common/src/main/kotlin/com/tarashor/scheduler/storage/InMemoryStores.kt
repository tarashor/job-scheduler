package com.tarashor.scheduler.storage

import com.tarashor.scheduler.core.model.*
import java.util.concurrent.ConcurrentHashMap

class InMemoryOutboxStore : OutboxStore {
    private val events = ConcurrentHashMap<String, OutboxEvent>()

    override fun saveOutboxEvent(event: OutboxEvent) {
        events[event.eventId] = event
    }

    override fun fetchPendingOutboxEvents(limit: Int): List<OutboxEvent> {
        return events.values
            .filter { it.status == OutboxStatus.PENDING }
            .sortedBy { it.createdAtEpochMs }
            .take(limit)
    }

    override fun markOutboxDispatched(eventId: String, dispatchedAtEpochMs: Long) {
        val current = events[eventId] ?: return
        events[eventId] = current.copy(
            status = OutboxStatus.DISPATCHED,
            dispatchedAtEpochMs = dispatchedAtEpochMs
        )
    }
}

class InMemoryJobMetadataStore : JobMetadataStore {
    private val jobs = ConcurrentHashMap<String, JobSpec>()
    override fun saveJob(job: JobSpec) { jobs[job.jobId] = job }
    override fun getJob(jobId: String): JobSpec? = jobs[jobId]
    override fun listJobs(): List<JobSpec> = jobs.values.sortedByDescending { it.createdAtEpochMs }
    override fun deleteJob(jobId: String): Boolean = jobs.remove(jobId) != null
}

class InMemoryRunHistoryStore : RunHistoryStore {
    private val runs = ConcurrentHashMap<String, JobRun>()
    private val taskInstances = ConcurrentHashMap<String, TaskInstance>()

    override fun saveRun(run: JobRun) { runs[run.runId] = run }
    override fun getRun(runId: String): JobRun? = runs[runId]
    override fun listRuns(limit: Int): List<JobRun> = runs.values.sortedByDescending { it.triggeredAtEpochMs }.take(limit)

    override fun saveTaskInstance(instance: TaskInstance) { taskInstances[instance.taskInstanceId] = instance }
    override fun getTaskInstance(taskInstanceId: String): TaskInstance? = taskInstances[taskInstanceId]
    override fun getTaskInstancesForRun(runId: String): List<TaskInstance> =
        taskInstances.values.filter { it.runId == runId }.sortedBy { it.scheduledAtEpochMs }
    override fun findActiveTaskInstances(): List<TaskInstance> =
        taskInstances.values.filter { it.status in setOf(TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.RETRYING) }
}

class InMemoryWorkerRegistry : WorkerRegistry {
    private val workers = ConcurrentHashMap<String, WorkerInfo>()
    override fun upsertWorker(worker: WorkerInfo) { workers[worker.workerId] = worker }
    override fun getWorker(workerId: String): WorkerInfo? = workers[workerId]
    override fun listWorkers(): List<WorkerInfo> = workers.values.sortedBy { it.workerId }
}

class InMemorySchedulerStorage : SchedulerStorage {
    private val jobs = ConcurrentHashMap<String, JobSpec>()
    private val runs = ConcurrentHashMap<String, JobRun>()
    private val taskInstances = ConcurrentHashMap<String, TaskInstance>()
    private val workers = ConcurrentHashMap<String, WorkerInfo>()
    private val outboxEvents = ConcurrentHashMap<String, OutboxEvent>()

    override fun saveJob(job: JobSpec) { jobs[job.jobId] = job }
    override fun getJob(jobId: String): JobSpec? = jobs[jobId]
    override fun listJobs(): List<JobSpec> = jobs.values.sortedByDescending { it.createdAtEpochMs }
    override fun deleteJob(jobId: String): Boolean = jobs.remove(jobId) != null

    override fun saveRun(run: JobRun) { runs[run.runId] = run }
    override fun getRun(runId: String): JobRun? = runs[runId]
    override fun listRuns(limit: Int): List<JobRun> = runs.values
        .sortedByDescending { it.triggeredAtEpochMs }
        .take(limit)

    override fun saveTaskInstance(instance: TaskInstance) { taskInstances[instance.taskInstanceId] = instance }
    override fun getTaskInstance(taskInstanceId: String): TaskInstance? = taskInstances[taskInstanceId]
    override fun getTaskInstancesForRun(runId: String): List<TaskInstance> =
        taskInstances.values.filter { it.runId == runId }.sortedBy { it.scheduledAtEpochMs }
    override fun findActiveTaskInstances(): List<TaskInstance> =
        taskInstances.values.filter { it.status in setOf(TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.RETRYING) }

    override fun upsertWorker(worker: WorkerInfo) { workers[worker.workerId] = worker }
    override fun getWorker(workerId: String): WorkerInfo? = workers[workerId]
    override fun listWorkers(): List<WorkerInfo> = workers.values.sortedBy { it.workerId }

    override fun saveOutboxEvent(event: OutboxEvent) { outboxEvents[event.eventId] = event }
    override fun fetchPendingOutboxEvents(limit: Int): List<OutboxEvent> =
        outboxEvents.values
            .filter { it.status == OutboxStatus.PENDING }
            .sortedBy { it.createdAtEpochMs }
            .take(limit)

    override fun markOutboxDispatched(eventId: String, dispatchedAtEpochMs: Long) {
        val current = outboxEvents[eventId] ?: return
        outboxEvents[eventId] = current.copy(
            status = OutboxStatus.DISPATCHED,
            dispatchedAtEpochMs = dispatchedAtEpochMs
        )
    }
}
