package com.tarashor.scheduler

import com.tarashor.scheduler.cluster.InMemoryLeaseStore
import com.tarashor.scheduler.coordinator.SchedulerCoordinator
import com.tarashor.scheduler.core.model.*
import com.tarashor.scheduler.queue.InMemoryTaskQueue
import com.tarashor.scheduler.storage.InMemorySchedulerStorage
import com.tarashor.scheduler.worker.WorkerNode
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class EndToEndSchedulerTest {

    @Test
    fun `test DAG workflow end-to-end execution`() = runBlocking {
        val storage = InMemorySchedulerStorage()
        val leaseStore = InMemoryLeaseStore()
        val taskQueue = InMemoryTaskQueue()

        val coordinator = SchedulerCoordinator(
            coordinatorId = "master-test",
            leaseStore = leaseStore,
            storage = storage,
            taskQueue = taskQueue,
            tickIntervalMs = 200
        )
        coordinator.start()

        val worker = WorkerNode(
            workerId = "worker-1",
            capacity = 2,
            taskQueue = taskQueue,
            storage = storage,
            heartbeatIntervalMs = 500,
            onTaskCompleted = { task, result ->
                coordinator.handleTaskCompletion(task, result)
            }
        )
        worker.start()

        // Wait for leader election and worker registration
        delay(1000)
        assertTrue(coordinator.isLeader())

        // Create DAG: A -> B -> C
        val job = JobSpec(
            jobId = "test-dag",
            name = "Test Sequential DAG",
            schedule = ScheduleSpec.Immediate,
            tasks = listOf(
                TaskSpec("task-A", "Step A", dependencies = emptySet(), action = TaskAction.Shell("echo A")),
                TaskSpec("task-B", "Step B", dependencies = setOf("task-A"), action = TaskAction.Shell("echo B")),
                TaskSpec("task-C", "Step C", dependencies = setOf("task-B"), action = TaskAction.Shell("echo C"))
            )
        )
        storage.saveJob(job)

        val run = coordinator.triggerJob("test-dag", triggerSource = "TEST")
        assertNotNull(run)

        // Poll until completion or timeout (max 10 seconds)
        var completed = false
        for (i in 1..20) {
            val currentRun = storage.getRun(run.runId)
            if (currentRun?.status == JobStatus.COMPLETED) {
                completed = true
                break
            }
            delay(500)
        }

        assertTrue(completed, "JobRun should have reached COMPLETED status")

        val taskInstances = storage.getTaskInstancesForRun(run.runId)
        assertEquals(3, taskInstances.size)
        assertTrue(taskInstances.all { it.status == TaskStatus.COMPLETED }, "All DAG task instances should be COMPLETED")

        // Verify ordering of execution
        val instA = taskInstances.first { it.taskId == "task-A" }
        val instB = taskInstances.first { it.taskId == "task-B" }
        val instC = taskInstances.first { it.taskId == "task-C" }

        assertTrue(instA.completedAtEpochMs!! <= instB.startedAtEpochMs!!, "Task A should complete before Task B starts")
        assertTrue(instB.completedAtEpochMs!! <= instC.startedAtEpochMs!!, "Task B should complete before Task C starts")

        worker.stop()
        coordinator.stop()
    }

    @Test
    fun `test dead worker detection and task reclamation`() = runBlocking {
        val storage = InMemorySchedulerStorage()
        val leaseStore = InMemoryLeaseStore()
        val taskQueue = InMemoryTaskQueue()

        val coordinator = SchedulerCoordinator(
            coordinatorId = "master-reaper",
            leaseStore = leaseStore,
            storage = storage,
            taskQueue = taskQueue,
            tickIntervalMs = 300,
            workerHeartbeatTimeoutMs = 1000 // Short heartbeat timeout for test
        )
        coordinator.start()
        delay(600)

        // Register a worker with a task, then simulate it crashing by stopping heartbeats
        val deadWorkerInfo = WorkerInfo(
            workerId = "crashed-worker",
            capacity = 1,
            currentLoad = 1,
            lastHeartbeatEpochMs = System.currentTimeMillis() - 2000, // 2s ago -> expired
            status = WorkerStatus.HEALTHY
        )
        storage.upsertWorker(deadWorkerInfo)

        val stuckTask = TaskInstance(
            taskInstanceId = "run-x-task-1-1",
            runId = "run-x",
            jobId = "job-x",
            taskId = "task-1",
            status = TaskStatus.RUNNING,
            assignedWorkerId = "crashed-worker",
            action = TaskAction.Shell("sleep 10"),
            scheduledAtEpochMs = System.currentTimeMillis()
        )
        storage.saveTaskInstance(stuckTask)

        // Wait for coordinator tick to reap the dead worker
        delay(1200)

        val updatedWorker = storage.getWorker("crashed-worker")
        assertNotNull(updatedWorker)
        assertEquals(WorkerStatus.DEAD, updatedWorker.status, "Worker should be marked DEAD")

        val updatedTask = storage.getTaskInstance("run-x-task-1-1")
        assertNotNull(updatedTask)
        assertEquals(TaskStatus.RETRYING, updatedTask.status, "Task should have been reclaimed and set to RETRYING")

        coordinator.stop()
    }
}
