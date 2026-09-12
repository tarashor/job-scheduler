package com.tarashor.scheduler.storage

import com.tarashor.scheduler.cluster.LeaseStore
import com.tarashor.scheduler.core.model.*
import com.tarashor.scheduler.queue.TaskQueue
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPool
import redis.clients.jedis.params.SetParams
import java.util.UUID
import kotlin.math.min
import kotlin.math.pow
import kotlin.random.Random

class RedisStorage(private val pool: JedisPool) : SchedulerStorage, LeaseStore, TaskQueue {
    private val logger = LoggerFactory.getLogger(RedisStorage::class.java)
    private val json = Json { ignoreUnknownKeys = true }

    // Keys
    private val keyLeaderLease = "scheduler:lease:leader"
    private val keyFencingSeq = "scheduler:lease:fencing_seq"
    private val keyJobs = "scheduler:jobs"
    private val keyRuns = "scheduler:runs"
    private val keyTaskInstances = "scheduler:instances"
    private val keyWorkers = "scheduler:workers"
    private val keyQueue = "scheduler:queue:ready"
    private val keyDlq = "scheduler:queue:dlq"

    // --- LEASE STORE (Leader Election) ---
    override fun getCurrentLease(): LeaderLease? {
        pool.resource.use { jedis ->
            val data = jedis.get(keyLeaderLease) ?: return null
            return json.decodeFromString<LeaderLease>(data)
        }
    }

    override fun tryAcquire(nodeId: String, durationMs: Long): LeaderLease? {
        pool.resource.use { jedis ->
            val token = jedis.incr(keyFencingSeq)
            val now = System.currentTimeMillis()
            val lease = LeaderLease(
                leaderId = nodeId,
                fencingToken = token,
                acquiredAtEpochMs = now,
                expiresAtEpochMs = now + durationMs
            )
            val payload = json.encodeToString(lease)
            val params = SetParams().nx().px(durationMs)
            val res = jedis.set(keyLeaderLease, payload, params)
            return if ("OK" == res) lease else null
        }
    }

    override fun tryRenew(nodeId: String, fencingToken: Long, durationMs: Long): LeaderLease? {
        pool.resource.use { jedis ->
            val current = getCurrentLease()
            if (current != null && current.leaderId == nodeId && current.fencingToken == fencingToken) {
                val now = System.currentTimeMillis()
                val renewed = current.copy(expiresAtEpochMs = now + durationMs)
                val params = SetParams().xx().px(durationMs)
                val res = jedis.set(keyLeaderLease, json.encodeToString(renewed), params)
                return if ("OK" == res) renewed else null
            }
            return null
        }
    }

    override fun release(nodeId: String, fencingToken: Long): Boolean {
        pool.resource.use { jedis ->
            val current = getCurrentLease()
            if (current != null && current.leaderId == nodeId && current.fencingToken == fencingToken) {
                jedis.del(keyLeaderLease)
                return true
            }
            return false
        }
    }

    // --- TASK QUEUE ---
    override suspend fun enqueue(task: TaskInstance): Unit = withContext(Dispatchers.IO) {
        val queued = task.copy(status = TaskStatus.QUEUED)
        val payload = json.encodeToString(queued)
        pool.resource.use { jedis ->
            // Score is scheduledAtEpochMs
            jedis.zadd(keyQueue, queued.scheduledAtEpochMs.toDouble(), payload)
        }
        logger.info("Redis: Enqueued task '${task.taskInstanceId}' scheduled for ${task.scheduledAtEpochMs}ms")
    }

    override suspend fun poll(maxWaitMs: Long): TaskInstance? = withContext(Dispatchers.IO) {
        val deadline = System.currentTimeMillis() + maxWaitMs
        while (System.currentTimeMillis() <= deadline) {
            val now = System.currentTimeMillis()
            pool.resource.use { jedis ->
                // Pop earliest task where scheduledAt <= now
                val items = jedis.zrangeByScore(keyQueue, 0.0, now.toDouble(), 0, 1)
                if (items.isNotEmpty()) {
                    val raw = items.first()
                    // Atomic remove
                    val removed = jedis.zrem(keyQueue, raw)
                    if (removed > 0) {
                        return@withContext json.decodeFromString<TaskInstance>(raw)
                    }
                }
            }
            kotlinx.coroutines.delay(min(100L, maxWaitMs))
        }
        return@withContext null
    }

    override suspend fun requeueWithBackoff(task: TaskInstance, reason: String): TaskInstance = withContext(Dispatchers.IO) {
        val nextAttempt = task.attempt + 1
        if (nextAttempt > task.maxRetries) {
            logger.warn("Task '${task.taskInstanceId}' exceeded max retries. Moving to Redis DLQ.")
            sendToDlq(task, "Exceeded max retries: $reason")
            return@withContext task.copy(status = TaskStatus.DEAD_LETTER, error = reason)
        }

        val baseMs = 1000L
        val maxBackoffMs = 60_000L
        val exponentialDelay = (baseMs * 2.0.pow((nextAttempt - 1).toDouble())).toLong()
        val cappedDelay = min(exponentialDelay, maxBackoffMs)
        val jitter = Random.nextLong(0, min(1000L, cappedDelay / 2 + 1))
        val totalDelay = cappedDelay + jitter

        val nextScheduledAt = System.currentTimeMillis() + totalDelay
        val retryingTask = task.copy(
            attempt = nextAttempt,
            status = TaskStatus.RETRYING,
            scheduledAtEpochMs = nextScheduledAt,
            assignedWorkerId = null,
            startedAtEpochMs = null,
            error = reason
        )

        enqueue(retryingTask)
        return@withContext retryingTask
    }

    override suspend fun sendToDlq(task: TaskInstance, reason: String): DeadLetterEntry = withContext(Dispatchers.IO) {
        val entry = DeadLetterEntry(
            id = UUID.randomUUID().toString(),
            taskInstance = task.copy(status = TaskStatus.DEAD_LETTER, error = reason),
            reason = reason,
            failedAtEpochMs = System.currentTimeMillis()
        )
        pool.resource.use { jedis ->
            jedis.hset(keyDlq, entry.id, json.encodeToString(entry))
        }
        return@withContext entry
    }

    override suspend fun getDlqEntries(): List<DeadLetterEntry> = withContext(Dispatchers.IO) {
        pool.resource.use { jedis ->
            jedis.hgetAll(keyDlq).values.map { json.decodeFromString<DeadLetterEntry>(it) }
                .sortedByDescending { it.failedAtEpochMs }
        }
    }

    override suspend fun retryDlqEntry(dlqEntryId: String): TaskInstance? = withContext(Dispatchers.IO) {
        pool.resource.use { jedis ->
            val raw = jedis.hget(keyDlq, dlqEntryId) ?: return@withContext null
            jedis.hdel(keyDlq, dlqEntryId)
            val entry = json.decodeFromString<DeadLetterEntry>(raw)
            val resetTask = entry.taskInstance.copy(
                status = TaskStatus.QUEUED,
                attempt = 1,
                error = null,
                scheduledAtEpochMs = System.currentTimeMillis()
            )
            enqueue(resetTask)
            return@withContext resetTask
        }
    }

    override suspend fun queueSize(): Int = withContext(Dispatchers.IO) {
        pool.resource.use { it.zcard(keyQueue).toInt() }
    }

    override suspend fun dlqSize(): Int = withContext(Dispatchers.IO) {
        pool.resource.use { it.hlen(keyDlq).toInt() }
    }

    // --- SCHEDULER STORAGE (Metadata) ---
    override fun saveJob(job: JobSpec) {
        pool.resource.use { it.hset(keyJobs, job.jobId, json.encodeToString(job)) }
    }

    override fun getJob(jobId: String): JobSpec? {
        pool.resource.use {
            val raw = it.hget(keyJobs, jobId) ?: return null
            return json.decodeFromString(raw)
        }
    }

    override fun listJobs(): List<JobSpec> {
        pool.resource.use {
            return it.hgetAll(keyJobs).values.map { raw -> json.decodeFromString<JobSpec>(raw) }
                .sortedByDescending { j -> j.createdAtEpochMs }
        }
    }

    override fun deleteJob(jobId: String): Boolean {
        pool.resource.use { return it.hdel(keyJobs, jobId) > 0 }
    }

    override fun saveRun(run: JobRun) {
        pool.resource.use { it.hset(keyRuns, run.runId, json.encodeToString(run)) }
    }

    override fun getRun(runId: String): JobRun? {
        pool.resource.use {
            val raw = it.hget(keyRuns, runId) ?: return null
            return json.decodeFromString(raw)
        }
    }

    override fun listRuns(limit: Int): List<JobRun> {
        pool.resource.use {
            return it.hgetAll(keyRuns).values.map { raw -> json.decodeFromString<JobRun>(raw) }
                .sortedByDescending { r -> r.triggeredAtEpochMs }
                .take(limit)
        }
    }

    override fun saveTaskInstance(instance: TaskInstance) {
        pool.resource.use { it.hset(keyTaskInstances, instance.taskInstanceId, json.encodeToString(instance)) }
    }

    override fun getTaskInstance(taskInstanceId: String): TaskInstance? {
        pool.resource.use {
            val raw = it.hget(keyTaskInstances, taskInstanceId) ?: return null
            return json.decodeFromString(raw)
        }
    }

    override fun getTaskInstancesForRun(runId: String): List<TaskInstance> {
        pool.resource.use {
            return it.hgetAll(keyTaskInstances).values
                .map { raw -> json.decodeFromString<TaskInstance>(raw) }
                .filter { inst -> inst.runId == runId }
                .sortedBy { inst -> inst.scheduledAtEpochMs }
        }
    }

    override fun findActiveTaskInstances(): List<TaskInstance> {
        pool.resource.use {
            return it.hgetAll(keyTaskInstances).values
                .map { raw -> json.decodeFromString<TaskInstance>(raw) }
                .filter { inst -> inst.status in setOf(TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.RETRYING) }
        }
    }

    override fun upsertWorker(worker: WorkerInfo) {
        pool.resource.use { it.hset(keyWorkers, worker.workerId, json.encodeToString(worker)) }
    }

    override fun getWorker(workerId: String): WorkerInfo? {
        pool.resource.use {
            val raw = it.hget(keyWorkers, workerId) ?: return null
            return json.decodeFromString(raw)
        }
    }

    override fun listWorkers(): List<WorkerInfo> {
        pool.resource.use {
            return it.hgetAll(keyWorkers).values
                .map { raw -> json.decodeFromString<WorkerInfo>(raw) }
                .sortedBy { w -> w.workerId }
        }
    }
}
