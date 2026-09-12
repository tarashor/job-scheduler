package com.tarashor.scheduler.api

import com.tarashor.scheduler.coordinator.SchedulerCoordinator
import com.tarashor.scheduler.core.model.*
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
    val coordinator: SchedulerCoordinator
) {
    private val logger = LoggerFactory.getLogger("SchedulerApiServer-$port")
    private var server: EmbeddedServer<*, *>? = null

    fun start(wait: Boolean = false) {
        logger.info("Starting Scheduler API Server on port $port")
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
                    call.respond(
                        ClusterHealthResponse(
                            coordinatorId = coordinator.coordinatorId,
                            isLeader = coordinator.isLeader(),
                            fencingToken = coordinator.getActiveFencingToken(),
                            queueSize = coordinator.taskQueue.queueSize(),
                            dlqSize = coordinator.taskQueue.dlqSize()
                        )
                    )
                }

                // Jobs
                get("/api/jobs") {
                    call.respond(coordinator.storage.listJobs())
                }

                get("/api/jobs/{id}") {
                    val id = call.parameters["id"] ?: return@get call.respond(HttpStatusCode.BadRequest)
                    val job = coordinator.storage.getJob(id)
                    if (job != null) call.respond(job) else call.respond(HttpStatusCode.NotFound)
                }

                post("/api/jobs") {
                    val job = call.receive<JobSpec>()
                    coordinator.storage.saveJob(job)
                    call.respond(HttpStatusCode.Created, job)
                }

                delete("/api/jobs/{id}") {
                    val id = call.parameters["id"] ?: return@delete call.respond(HttpStatusCode.BadRequest)
                    val deleted = coordinator.storage.deleteJob(id)
                    if (deleted) call.respond(HttpStatusCode.OK) else call.respond(HttpStatusCode.NotFound)
                }

                post("/api/jobs/{id}/trigger") {
                    val id = call.parameters["id"] ?: return@post call.respond(HttpStatusCode.BadRequest)
                    try {
                        val run = coordinator.triggerJob(id, triggerSource = "MANUAL_API")
                        call.respond(HttpStatusCode.Accepted, run)
                    } catch (e: Exception) {
                        call.respond(HttpStatusCode.BadRequest, mapOf("error" to (e.message ?: "Failed to trigger job")))
                    }
                }

                // Runs
                get("/api/runs") {
                    val runs = coordinator.storage.listRuns(50)
                    val enriched = runs.map { run ->
                        val tasks = coordinator.storage.getTaskInstancesForRun(run.runId)
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
                    val run = coordinator.storage.getRun(id)
                    if (run == null) {
                        call.respond(HttpStatusCode.NotFound)
                    } else {
                        val tasks = coordinator.storage.getTaskInstancesForRun(run.runId)
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
                    call.respond(coordinator.storage.listWorkers())
                }

                // DLQ
                get("/api/dlq") {
                    call.respond(coordinator.taskQueue.getDlqEntries())
                }

                post("/api/dlq/{id}/retry") {
                    val id = call.parameters["id"] ?: return@post call.respond(HttpStatusCode.BadRequest)
                    val retried = coordinator.taskQueue.retryDlqEntry(id)
                    if (retried != null) {
                        coordinator.storage.saveTaskInstance(retried)
                        call.respond(HttpStatusCode.OK, retried)
                    } else {
                        call.respond(HttpStatusCode.NotFound)
                    }
                }

                // Failover trigger
                post("/api/cluster/stepdown") {
                    coordinator.leaderElector.stepDown()
                    call.respond(HttpStatusCode.OK, mapOf("status" to "Leader stepped down"))
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
                    coordinator.storage.saveJob(sampleJob)
                    val run = coordinator.triggerJob(jobId, triggerSource = "SAMPLE_DAG_TRIGGER")
                    call.respond(HttpStatusCode.Created, run)
                }
            }
        }.start(wait = wait)
    }

    fun stop() {
        server?.stop(1000, 2000)
    }
}
