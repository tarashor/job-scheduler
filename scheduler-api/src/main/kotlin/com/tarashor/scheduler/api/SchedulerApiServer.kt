package com.tarashor.scheduler.api

import com.tarashor.scheduler.cluster.LeaseStore
import com.tarashor.scheduler.core.dag.DAGEngine
import com.tarashor.scheduler.core.model.*
import com.tarashor.scheduler.queue.TaskQueue
import com.tarashor.scheduler.storage.CompositeSchedulerStorage
import com.tarashor.scheduler.storage.JobMetadataStore
import com.tarashor.scheduler.storage.RunHistoryStore
import com.tarashor.scheduler.storage.SchedulerStorage
import com.tarashor.scheduler.storage.WorkerRegistry
import com.tarashor.scheduler.ui.DashboardHtml
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.cio.*
import io.ktor.server.engine.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.cors.routing.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
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

class SchedulerApiServer(
    val port: Int,
    val jobMetadataStore: JobMetadataStore,
    val runHistoryStore: RunHistoryStore,
    val workerRegistry: WorkerRegistry,
    val taskQueue: TaskQueue,
    val leaseStore: LeaseStore
) {
    val storage: SchedulerStorage = CompositeSchedulerStorage(jobMetadataStore, runHistoryStore, workerRegistry)

    // Backward-compatible constructor
    constructor(
        port: Int,
        storage: SchedulerStorage,
        taskQueue: TaskQueue,
        leaseStore: LeaseStore
    ) : this(
        port = port,
        jobMetadataStore = storage,
        runHistoryStore = storage,
        workerRegistry = storage,
        taskQueue = taskQueue,
        leaseStore = leaseStore
    )
    private val logger = LoggerFactory.getLogger("SchedulerApiServer-$port")
    private var server: EmbeddedServer<*, *>? = null

    fun start(wait: Boolean = false) {
        logger.info("Starting Scheduler API Server microservice on port $port")
        server = embeddedServer(CIO, port = port) {
            install(ContentNegotiation) {
                json(Json {
                    prettyPrint = true
                    isLenient = true
                    ignoreUnknownKeys = true
                })
            }
            install(CORS) {
                anyHost()
                allowHeader(HttpHeaders.ContentType)
                allowMethod(HttpMethod.Get)
                allowMethod(HttpMethod.Post)
                allowMethod(HttpMethod.Delete)
            }

            routing {
                // UI Dashboard
                get("/") {
                    call.respondText(DashboardHtml.render(), ContentType.Text.Html)
                }
                get("/ui") {
                    call.respondText(DashboardHtml.render(), ContentType.Text.Html)
                }

                // Health & Cluster state
                get("/api/health") {
                    val lease = leaseStore.getCurrentLease()
                    call.respond(
                        ClusterHealthResponse(
                            coordinatorId = lease?.leaderId ?: "standby-mode",
                            isLeader = lease != null,
                            fencingToken = lease?.fencingToken ?: 0L,
                            queueSize = taskQueue.queueSize(),
                            dlqSize = taskQueue.dlqSize()
                        )
                    )
                }

                // Jobs
                get("/api/jobs") {
                    call.respond(storage.listJobs())
                }

                get("/api/jobs/{id}") {
                    val id = call.parameters["id"] ?: return@get call.respond(HttpStatusCode.BadRequest)
                    val job = storage.getJob(id)
                    if (job != null) call.respond(job) else call.respond(HttpStatusCode.NotFound)
                }

                post("/api/jobs") {
                    val job = call.receive<JobSpec>()
                    // Validate DAG
                    try {
                        DAGEngine.validateAndSort(job.tasks)
                    } catch (e: Exception) {
                        return@post call.respond(HttpStatusCode.BadRequest, mapOf("error" to (e.message ?: "Invalid DAG")))
                    }
                    storage.saveJob(job)
                    call.respond(HttpStatusCode.Created, job)
                }

                delete("/api/jobs/{id}") {
                    val id = call.parameters["id"] ?: return@delete call.respond(HttpStatusCode.BadRequest)
                    val deleted = storage.deleteJob(id)
                    if (deleted) call.respond(HttpStatusCode.OK) else call.respond(HttpStatusCode.NotFound)
                }

                post("/api/jobs/{id}/trigger") {
                    val id = call.parameters["id"] ?: return@post call.respond(HttpStatusCode.BadRequest)
                    try {
                        val run = triggerJob(id, triggerSource = "MANUAL_API")
                        call.respond(HttpStatusCode.Accepted, run)
                    } catch (e: Exception) {
                        call.respond(HttpStatusCode.BadRequest, mapOf("error" to (e.message ?: "Failed to trigger job")))
                    }
                }

                // Runs
                get("/api/runs") {
                    val runs = storage.listRuns(50)
                    val enriched = runs.map { run ->
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
                    call.respond(enriched)
                }

                get("/api/runs/{id}") {
                    val id = call.parameters["id"] ?: return@get call.respond(HttpStatusCode.BadRequest)
                    val run = storage.getRun(id)
                    if (run == null) {
                        call.respond(HttpStatusCode.NotFound)
                    } else {
                        val tasks = storage.getTaskInstancesForRun(run.runId)
                        call.respond(
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
                }

                // Workers
                get("/api/workers") {
                    call.respond(storage.listWorkers())
                }

                // DLQ
                get("/api/dlq") {
                    call.respond(taskQueue.getDlqEntries())
                }

                post("/api/dlq/{id}/retry") {
                    val id = call.parameters["id"] ?: return@post call.respond(HttpStatusCode.BadRequest)
                    val retried = taskQueue.retryDlqEntry(id)
                    if (retried != null) {
                        storage.saveTaskInstance(retried)
                        call.respond(HttpStatusCode.OK, retried)
                    } else {
                        call.respond(HttpStatusCode.NotFound)
                    }
                }

                // Failover trigger
                post("/api/cluster/stepdown") {
                    val lease = leaseStore.getCurrentLease()
                    if (lease != null) {
                        leaseStore.release(lease.leaderId, lease.fencingToken)
                        call.respond(HttpStatusCode.OK, mapOf("status" to "Leader lease released for failover"))
                    } else {
                        call.respond(HttpStatusCode.OK, mapOf("status" to "No active leader lease held"))
                    }
                }

                // Sample DAG Trigger Helper
                post("/api/samples/dag") {
                    val jobId = "sample-etl-dag"
                    val sampleJob = JobSpec(
                        jobId = jobId,
                        name = "E-Commerce Daily ETL Pipeline",
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
                                dependencies = setOf("extract-sales", "extract-inventory"),
                                action = TaskAction.Simulate(durationMs = 800, shouldFail = false, message = "Aggregated daily revenue and inventory turnover"),
                                timeoutMs = 10_000
                            ),
                            TaskSpec(
                                taskId = "load-warehouse",
                                name = "Load into Data Warehouse",
                                dependencies = setOf("transform-metrics"),
                                action = TaskAction.Shell("echo 'Successfully loaded 1820 rows into Data Warehouse'"),
                                timeoutMs = 10_000
                            )
                        )
                    )
                    storage.saveJob(sampleJob)
                    val run = triggerJob(jobId, triggerSource = "SAMPLE_DAG_TRIGGER")
                    call.respond(HttpStatusCode.Created, run)
                }
            }
        }.start(wait = wait)
    }

    suspend fun triggerJob(jobId: String, triggerSource: String = "MANUAL"): JobRun {
        val job = storage.getJob(jobId) ?: throw IllegalArgumentException("Job '$jobId' not found")
        val runId = UUID.randomUUID().toString()
        val now = System.currentTimeMillis()
        val fencingToken = leaseStore.getCurrentLease()?.fencingToken ?: 1L

        DAGEngine.validateAndSort(job.tasks)

        val run = JobRun(
            runId = runId,
            jobId = jobId,
            status = JobStatus.RUNNING,
            triggeredAtEpochMs = now,
            startedAtEpochMs = now,
            triggerSource = triggerSource
        )
        storage.saveRun(run)

        val initialInstances = mutableMapOf<String, TaskInstance>()
        for (taskSpec in job.tasks) {
            val instance = TaskInstance(
                taskInstanceId = "$runId-${taskSpec.taskId}-1",
                runId = runId,
                jobId = jobId,
                taskId = taskSpec.taskId,
                status = TaskStatus.WAITING_DEPENDENCIES,
                attempt = 1,
                maxRetries = taskSpec.maxRetries,
                action = taskSpec.action,
                scheduledAtEpochMs = now,
                fencingToken = fencingToken
            )
            storage.saveTaskInstance(instance)
            initialInstances[taskSpec.taskId] = instance
        }

        val readyTasks = DAGEngine.findReadyTasks(job.tasks, initialInstances)
        for (task in readyTasks) {
            val inst = initialInstances[task.taskId]!!
            val queuedInst = inst.copy(status = TaskStatus.QUEUED)
            storage.saveTaskInstance(queuedInst)
            taskQueue.enqueue(queuedInst)
        }

        return run
    }

    fun stop() {
        server?.stop(1000, 2000)
    }
}
