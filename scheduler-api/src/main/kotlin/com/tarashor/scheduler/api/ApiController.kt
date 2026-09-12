package com.tarashor.scheduler.api

import com.tarashor.scheduler.cluster.LeaseStore
import com.tarashor.scheduler.core.model.*
import com.tarashor.scheduler.queue.TaskQueue
import com.tarashor.scheduler.storage.SchedulerStorage
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.util.UUID

@Serializable
data class ClusterHealthResponse(
    val coordinatorId: String,
    val isLeader: Boolean,
    val fencingToken: Long,
    val queueSize: Int,
    val dlqSize: Int
)

@Serializable
data class EnrichedJobRun(
    val runId: String,
    val jobId: String,
    val status: JobStatus,
    val triggeredAtEpochMs: Long,
    val startedAtEpochMs: Long? = null,
    val completedAtEpochMs: Long? = null,
    val triggerSource: String = "MANUAL",
    val error: String? = null,
    val taskInstances: List<TaskInstance> = emptyList()
)

@RestController
@RequestMapping("/api")
class ApiController(
    private val storage: SchedulerStorage,
    private val taskQueue: TaskQueue,
    private val leaseStore: LeaseStore
) {
    private val logger = LoggerFactory.getLogger(ApiController::class.java)

    @GetMapping("/health")
    fun health(): ClusterHealthResponse = runBlocking {
        val lease = leaseStore.getCurrentLease()
        ClusterHealthResponse(
            coordinatorId = lease?.leaderId ?: "standby-mode",
            isLeader = lease != null,
            fencingToken = lease?.fencingToken ?: 0L,
            queueSize = taskQueue.queueSize(),
            dlqSize = taskQueue.dlqSize()
        )
    }

    @GetMapping("/jobs")
    fun listJobs(): List<JobSpec> = storage.listJobs()

    @GetMapping("/jobs/{id}")
    fun getJob(@PathVariable id: String): ResponseEntity<JobSpec> {
        val job = storage.getJob(id) ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(job)
    }

    @PostMapping("/jobs")
    fun createJob(@RequestBody job: JobSpec): ResponseEntity<JobSpec> {
        storage.saveJob(job)
        return ResponseEntity.status(HttpStatus.CREATED).body(job)
    }

    @DeleteMapping("/jobs/{id}")
    fun deleteJob(@PathVariable id: String): ResponseEntity<Void> {
        val deleted = storage.deleteJob(id)
        return if (deleted) ResponseEntity.ok().build() else ResponseEntity.notFound().build()
    }

    @PostMapping("/jobs/{id}/trigger")
    fun triggerJobEndpoint(@PathVariable id: String): ResponseEntity<Any> = runBlocking {
        try {
            val run = triggerJob(id, triggerSource = "MANUAL_API")
            ResponseEntity.status(HttpStatus.ACCEPTED).body(run)
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(mapOf("error" to (e.message ?: "Failed to trigger job")))
        }
    }

    @GetMapping("/runs")
    fun listRuns(): List<EnrichedJobRun> {
        val runs = storage.listRuns(50)
        return runs.map { run ->
            val tasks = storage.getTaskInstancesForRun(run.runId)
            EnrichedJobRun(
                runId = run.runId,
                jobId = run.jobId,
                status = run.status,
                triggeredAtEpochMs = run.triggeredAtEpochMs,
                startedAtEpochMs = run.startedAtEpochMs,
                completedAtEpochMs = run.completedAtEpochMs,
                triggerSource = run.triggerSource,
                error = run.error,
                taskInstances = tasks
            )
        }
    }

    @GetMapping("/runs/{id}")
    fun getRun(@PathVariable id: String): ResponseEntity<EnrichedJobRun> {
        val run = storage.getRun(id) ?: return@getRun ResponseEntity.notFound().build()
        val tasks = storage.getTaskInstancesForRun(run.runId)
        return@getRun ResponseEntity.ok(
            EnrichedJobRun(
                runId = run.runId,
                jobId = run.jobId,
                status = run.status,
                triggeredAtEpochMs = run.triggeredAtEpochMs,
                startedAtEpochMs = run.startedAtEpochMs,
                completedAtEpochMs = run.completedAtEpochMs,
                triggerSource = run.triggerSource,
                error = run.error,
                taskInstances = tasks
            )
        )
    }

    @GetMapping("/workers")
    fun listWorkers(): List<WorkerInfo> = storage.listWorkers()

    @GetMapping("/dlq")
    fun getDlq(): List<DeadLetterEntry> = runBlocking { taskQueue.getDlqEntries() }

    @PostMapping("/dlq/{id}/retry")
    fun retryDlq(@PathVariable id: String): ResponseEntity<TaskInstance> = runBlocking {
        val retried = taskQueue.retryDlqEntry(id) ?: return@runBlocking ResponseEntity.notFound().build()
        storage.saveTaskInstance(retried)
        ResponseEntity.ok(retried)
    }

    @PostMapping("/cluster/stepdown")
    fun stepDown(): Map<String, String> {
        val lease = leaseStore.getCurrentLease()
        return if (lease != null) {
            leaseStore.release(lease.leaderId, lease.fencingToken)
            mapOf("status" to "Leader lease released for failover")
        } else {
            mapOf("status" to "No active leader lease held")
        }
    }

    @PostMapping("/samples/job", "/samples/dag")
    fun triggerSampleJob(): ResponseEntity<JobRun> = runBlocking {
        val jobId = "sample-parallel-job"
        val sampleJob = JobSpec(
            jobId = jobId,
            name = "E-Commerce Batch Processing Job",
            schedule = ScheduleSpec.Immediate,
            tasks = listOf(
                TaskSpec(
                    taskId = "extract-sales",
                    name = "Extract Sales Data",
                    action = TaskAction.Shell("echo 'Extracted 1500 sales records'"),
                    timeoutMs = 10_000
                ),
                TaskSpec(
                    taskId = "extract-inventory",
                    name = "Extract Inventory Data",
                    action = TaskAction.Shell("echo 'Extracted 320 inventory items'"),
                    timeoutMs = 10_000
                ),
                TaskSpec(
                    taskId = "transform-metrics",
                    name = "Transform & Aggregate",
                    action = TaskAction.Simulate(durationMs = 800, shouldFail = false, message = "Aggregated daily revenue and inventory turnover"),
                    timeoutMs = 10_000
                ),
                TaskSpec(
                    taskId = "load-warehouse",
                    name = "Load into Data Warehouse",
                    action = TaskAction.Shell("echo 'Successfully loaded 1820 rows into Data Warehouse'"),
                    timeoutMs = 10_000
                )
            )
        )
        storage.saveJob(sampleJob)
        val run = triggerJob(jobId, triggerSource = "SAMPLE_JOB_TRIGGER")
        ResponseEntity.status(HttpStatus.CREATED).body(run)
    }

    suspend fun triggerJob(jobId: String, triggerSource: String = "MANUAL"): JobRun {
        val job = storage.getJob(jobId) ?: throw IllegalArgumentException("Job '$jobId' not found")
        val runId = UUID.randomUUID().toString()
        val now = System.currentTimeMillis()
        val fencingToken = leaseStore.getCurrentLease()?.fencingToken ?: 1L

        logger.info("Triggering job '$jobId' (RunId: $runId, Source: $triggerSource, FencingToken: $fencingToken)")

        val run = JobRun(
            runId = runId,
            jobId = jobId,
            status = JobStatus.RUNNING,
            triggeredAtEpochMs = now,
            startedAtEpochMs = now,
            triggerSource = triggerSource
        )
        storage.saveRun(run)

        for (taskSpec in job.tasks) {
            val instance = TaskInstance(
                taskInstanceId = "$runId-${taskSpec.taskId}-1",
                runId = runId,
                jobId = jobId,
                taskId = taskSpec.taskId,
                status = TaskStatus.QUEUED,
                attempt = 1,
                maxRetries = taskSpec.maxRetries,
                action = taskSpec.action,
                scheduledAtEpochMs = now,
                fencingToken = fencingToken
            )
            val outboxEvent = OutboxEvent(
                eventId = UUID.randomUUID().toString(),
                aggregateId = instance.taskInstanceId,
                taskInstance = instance
            )
            storage.saveTaskInstance(instance)
            storage.saveOutboxEvent(outboxEvent)
            taskQueue.enqueue(instance)
            storage.markOutboxDispatched(outboxEvent.eventId)
        }

        return run
    }
}
