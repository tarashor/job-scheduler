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

class StatelessCoordinatorTest {

    @Test
    fun `test multiple active-active stateless coordinators schedule without duplicate executions`() = runBlocking {
        val sharedStorage = InMemorySchedulerStorage()
        val sharedLeaseStore = InMemoryLeaseStore()
        val sharedTaskQueue = InMemoryTaskQueue()

        // 1. Launch two peer stateless coordinator pods concurrently
        val coordPod1 = SchedulerCoordinator(
            coordinatorId = "coord-pod-1",
            leaseStore = sharedLeaseStore,
            storage = sharedStorage,
            taskQueue = sharedTaskQueue,
            tickIntervalMs = 100,
            statelessActiveActive = true
        )
        val coordPod2 = SchedulerCoordinator(
            coordinatorId = "coord-pod-2",
            leaseStore = sharedLeaseStore,
            storage = sharedStorage,
            taskQueue = sharedTaskQueue,
            tickIntervalMs = 100,
            statelessActiveActive = true
        )

        coordPod1.start()
        coordPod2.start()

        // Both pods are active peers simultaneously
        assertTrue(coordPod1.isLeader(), "Pod 1 should be active")
        assertTrue(coordPod2.isLeader(), "Pod 2 should be active")

        // 2. Launch a stateless worker
        val worker = WorkerNode(
            workerId = "stateless-worker-test",
            capacity = 4,
            taskQueue = sharedTaskQueue,
            storage = sharedStorage,
            heartbeatIntervalMs = 500
        )
        worker.start()

        // 3. Register an immediate job that both pods will discover concurrently
        val immediateJob = JobSpec(
            jobId = "stateless-immediate-job",
            name = "Stateless Immediate Job",
            schedule = ScheduleSpec.Immediate,
            action = JobAction.Shell("echo 'Executed by stateless peer'")
        )
        sharedStorage.saveJob(immediateJob)

        // Wait for one of the peer pods to acquire the lock, trigger, and worker to complete
        var runCompleted = false
        for (i in 1..25) {
            val runs = sharedStorage.listRuns().filter { it.jobId == "stateless-immediate-job" }
            if (runs.isNotEmpty() && runs.first().status == JobStatus.COMPLETED) {
                runCompleted = true
                break
            }
            delay(100)
        }

        assertTrue(runCompleted, "Immediate job should complete via stateless peer dispatch")

        // Crucial verification: EXACTLY 1 run must exist (no duplicate dispatch across active-active peers)
        val allRuns = sharedStorage.listRuns().filter { it.jobId == "stateless-immediate-job" }
        assertEquals(1, allRuns.size, "Exactly one run must be executed despite multiple active coordinators")

        val tasks = sharedStorage.getTaskInstancesForRun(allRuns.first().runId)
        assertEquals(1, tasks.size)
        assertEquals(JobStatus.COMPLETED, tasks.first().status)

        worker.stop()
        coordPod1.stop()
        coordPod2.stop()
    }

    @Test
    fun `test seamless zero-downtime failover between stateless coordinator peers`() = runBlocking {
        val sharedStorage = InMemorySchedulerStorage()
        val sharedLeaseStore = InMemoryLeaseStore()
        val sharedTaskQueue = InMemoryTaskQueue()

        val pod1 = SchedulerCoordinator(
            coordinatorId = "pod-1",
            leaseStore = sharedLeaseStore,
            storage = sharedStorage,
            taskQueue = sharedTaskQueue,
            tickIntervalMs = 100,
            statelessActiveActive = true
        )
        val pod2 = SchedulerCoordinator(
            coordinatorId = "pod-2",
            leaseStore = sharedLeaseStore,
            storage = sharedStorage,
            taskQueue = sharedTaskQueue,
            tickIntervalMs = 100,
            statelessActiveActive = true
        )

        pod1.start()
        pod2.start()

        // Crash pod 1 immediately
        pod1.stop()

        // Pod 2 seamlessly triggers and coordinates without any leader election delay
        val job = JobSpec(
            jobId = "manual-test-job-on-pod2",
            name = "Pod 2 Job",
            schedule = ScheduleSpec.Immediate,
            action = JobAction.Shell("echo pod2")
        )
        sharedStorage.saveJob(job)
        val run = pod2.triggerJob("manual-test-job-on-pod2", triggerSource = "FAILOVER_TEST")

        assertNotNull(run)
        assertEquals(JobStatus.RUNNING, run.status)
        assertEquals(1, sharedTaskQueue.queueSize())

        pod2.stop()
    }
}
