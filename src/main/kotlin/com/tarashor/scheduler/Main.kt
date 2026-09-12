package com.tarashor.scheduler

import com.tarashor.scheduler.api.SchedulerApiServer
import com.tarashor.scheduler.cluster.InMemoryLeaseStore
import com.tarashor.scheduler.coordinator.SchedulerCoordinator
import com.tarashor.scheduler.core.model.*
import com.tarashor.scheduler.queue.InMemoryTaskQueue
import com.tarashor.scheduler.storage.SqliteSchedulerStorage
import com.tarashor.scheduler.worker.WorkerNode
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.util.UUID
import kotlin.system.exitProcess

fun main(args: Array<String>) = runBlocking {
    val logger = LoggerFactory.getLogger("JobSchedulerMain")
    val mode = if (args.isNotEmpty()) args[0] else "cluster"

    when (mode) {
        "cluster" -> {
            println("""
======================================================================
  🚀 STARTING DISTRIBUTED JOB SCHEDULER (MICROSERVICES CLUSTER)
======================================================================
- Master Coordinator: active-master (Leader election with fencing tokens)
- Workers: 2 worker daemon nodes (worker-alpha, worker-beta, capacity 4 each)
- Storage: SQLite persistent store (scheduler.db)
- Microservices REST API & Web UI: http://localhost:8080
======================================================================
            """.trimIndent())

            val storage = SqliteSchedulerStorage("scheduler.db")
            val leaseStore = InMemoryLeaseStore()
            val taskQueue = InMemoryTaskQueue()

            // 1. Initialize Master Coordinator
            val coordinator = SchedulerCoordinator(
                coordinatorId = "active-master",
                leaseStore = leaseStore,
                storage = storage,
                taskQueue = taskQueue
            )
            coordinator.start()

            // 2. Initialize Worker Daemons
            val worker1 = WorkerNode(
                workerId = "worker-alpha",
                capacity = 4,
                taskQueue = taskQueue,
                storage = storage,
                onTaskCompleted = { task, result ->
                    coordinator.handleTaskCompletion(task, result)
                }
            )
            worker1.start()

            val worker2 = WorkerNode(
                workerId = "worker-beta",
                capacity = 4,
                taskQueue = taskQueue,
                storage = storage,
                onTaskCompleted = { task, result ->
                    coordinator.handleTaskCompletion(task, result)
                }
            )
            worker2.start()

            // 3. Register a sample recurring Cron Job and sample DAG Job
            val cronJob = JobSpec(
                jobId = "system-heartbeat-cron",
                name = "System Heartbeat & Healthcheck",
                schedule = ScheduleSpec.Cron("*/5 * * * *"), // every 5 minutes
                tasks = listOf(
                    TaskSpec(
                        taskId = "check-disk",
                        name = "Check Disk Free Space",
                        action = TaskAction.Shell("df -h / | tail -1"),
                        timeoutMs = 5000
                    )
                )
            )
            storage.saveJob(cronJob)

            val etlDag = JobSpec(
                jobId = "sample-etl-dag",
                name = "Daily Sales & Inventory ETL DAG",
                schedule = ScheduleSpec.Immediate,
                tasks = listOf(
                    TaskSpec(
                        taskId = "extract-sales",
                        name = "Extract Sales",
                        action = TaskAction.Shell("echo 'Sales batch extracted: 1200 rows'"),
                        timeoutMs = 10000
                    ),
                    TaskSpec(
                        taskId = "extract-inventory",
                        name = "Extract Inventory",
                        action = TaskAction.Shell("echo 'Inventory batch extracted: 450 rows'"),
                        timeoutMs = 10000
                    ),
                    TaskSpec(
                        taskId = "transform-metrics",
                        name = "Transform & Aggregate",
                        dependencies = setOf("extract-sales", "extract-inventory"),
                        action = TaskAction.Simulate(durationMs = 600, shouldFail = false, message = "Metrics computed"),
                        timeoutMs = 10000
                    ),
                    TaskSpec(
                        taskId = "load-warehouse",
                        name = "Load into Data Warehouse",
                        dependencies = setOf("transform-metrics"),
                        action = TaskAction.Shell("echo 'Warehouse loaded successfully with 1650 records'"),
                        timeoutMs = 10000
                    )
                )
            )
            storage.saveJob(etlDag)

            // 4. Start REST API Server & Web UI
            val server = SchedulerApiServer(port = 8080, coordinator = coordinator)
            server.start(wait = false)

            println("Microservices REST API & Web UI is live at: http://localhost:8080/")
            println("Press Ctrl+C to terminate the cluster.")

            Runtime.getRuntime().addShutdownHook(Thread {
                println("Shutting down cluster...")
                server.stop()
                worker1.stop()
                worker2.stop()
                runBlocking { coordinator.stop() }
            })

            while (true) {
                delay(10_000)
            }
        }
        "master" -> {
            val port = System.getenv("PORT")?.toIntOrNull() ?: 8080
            val masterId = System.getenv("MASTER_ID") ?: "master-${UUID.randomUUID().toString().substring(0, 6)}"
            println("Starting Scheduler Master microservice [$masterId] on port $port...")

            val storage = SqliteSchedulerStorage("scheduler.db")
            val leaseStore = InMemoryLeaseStore()
            val taskQueue = InMemoryTaskQueue()

            val coordinator = SchedulerCoordinator(
                coordinatorId = masterId,
                leaseStore = leaseStore,
                storage = storage,
                taskQueue = taskQueue
            )
            coordinator.start()

            val server = SchedulerApiServer(port = port, coordinator = coordinator)
            server.start(wait = true)
        }
        "worker" -> {
            val workerId = System.getenv("WORKER_ID") ?: "worker-${UUID.randomUUID().toString().substring(0, 6)}"
            val capacity = System.getenv("WORKER_CAPACITY")?.toIntOrNull() ?: 4
            println("Starting Worker microservice daemon [$workerId] with capacity $capacity...")

            val storage = SqliteSchedulerStorage("scheduler.db")
            val taskQueue = InMemoryTaskQueue()

            val worker = WorkerNode(
                workerId = workerId,
                capacity = capacity,
                taskQueue = taskQueue,
                storage = storage
            )
            worker.start()

            while (true) {
                delay(10_000)
            }
        }
        else -> {
            println("Unknown mode: '$mode'. Available modes: 'cluster', 'master', 'worker'.")
            exitProcess(1)
        }
    }
}
