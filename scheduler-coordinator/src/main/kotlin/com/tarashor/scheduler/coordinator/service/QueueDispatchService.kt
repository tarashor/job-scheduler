package com.tarashor.scheduler.coordinator.service

import com.tarashor.scheduler.core.model.*
import com.tarashor.scheduler.core.ratelimit.QueueRateLimiterRegistry
import com.tarashor.scheduler.queue.TaskQueue
import com.tarashor.scheduler.storage.QueueStore
import com.tarashor.scheduler.storage.RunHistoryStore
import com.tarashor.scheduler.storage.TaskStore
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/**
 * Single Responsibility: Push-based Cloud Tasks Dispatcher.
 * Dispatches tasks directly to HTTP endpoints respecting:
 * - Queue state (skips PAUSED queues)
 * - Rate limits (Token Bucket maxDispatchesPerSecond)
 * - Concurrency limits (maxConcurrentDispatches)
 * - Automatic exponential retries on non-2xx HTTP responses
 */
interface QueueDispatchService {
    suspend fun dispatchPushTasks()
}

class DefaultQueueDispatchService(
    private val queueStore: QueueStore,
    private val taskStore: TaskStore,
    private val runHistoryStore: RunHistoryStore,
    private val taskQueue: TaskQueue,
    private val rateLimiterRegistry: QueueRateLimiterRegistry = QueueRateLimiterRegistry(),
    private val httpClient: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build()
) : QueueDispatchService {
    private val logger = LoggerFactory.getLogger(DefaultQueueDispatchService::class.java)

    override suspend fun dispatchPushTasks() {
        val polled = taskQueue.poll(lookaheadMs = 0L, maxWaitMs = 50L) ?: return
        val queueId = polled.jobId
        val queue = queueStore.getQueue(queueId) ?: QueueSpec(queueId = queueId)

        if (queue.state != QueueState.RUNNING) {
            // Queue is paused: requeue task and defer
            taskQueue.enqueue(polled)
            return
        }

        if (!rateLimiterRegistry.canDispatch(queue)) {
            // Rate limit or concurrency limit exceeded: requeue task and wait
            taskQueue.enqueue(polled)
            return
        }

        rateLimiterRegistry.onTaskDispatched(queue.queueId)
        try {
            when (val action = polled.action) {
                is JobAction.Http -> dispatchHttp(polled, action, queue)
                else -> rateLimiterRegistry.onTaskFinished(queue.queueId)
            }
        } catch (e: Exception) {
            rateLimiterRegistry.onTaskFinished(queue.queueId)
            logger.error("Error dispatching task '${polled.taskInstanceId}'", e)
        }
    }


    private suspend fun dispatchHttp(task: TaskInstance, httpAction: JobAction.Http, queue: QueueSpec) = withContext(Dispatchers.IO) {
        logger.info("Cloud Tasks Dispatcher: Sending HTTP ${httpAction.method} request for task '${task.taskInstanceId}' to ${httpAction.url}")

        val now = System.currentTimeMillis()
        try {
            val requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(httpAction.url))
                .timeout(Duration.ofSeconds(15))

            httpAction.headers.forEach { (k, v) -> requestBuilder.header(k, v) }

            val bodyPublisher = if (httpAction.body != null) {
                HttpRequest.BodyPublishers.ofString(httpAction.body)
            } else {
                HttpRequest.BodyPublishers.noBody()
            }

            requestBuilder.method(httpAction.method, bodyPublisher)

            val response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString())
            val statusCode = response.statusCode()
            val isSuccess = statusCode in 200..299

            if (isSuccess) {
                val completed = task.copy(
                    status = TaskStatus.COMPLETED,
                    completedAtEpochMs = System.currentTimeMillis(),
                    output = "HTTP $statusCode: ${response.body()}"
                )
                runHistoryStore.saveTaskInstance(completed)
                updateCloudTask(task.taskInstanceId, TaskStatus.COMPLETED, statusCode, response.body())
                logger.info("Cloud Task '${task.taskInstanceId}' executed successfully (HTTP $statusCode)")
            } else {
                handleFailure(task, queue, "HTTP status $statusCode: ${response.body()}", statusCode)
            }
        } catch (e: Exception) {
            handleFailure(task, queue, e.message ?: e.javaClass.simpleName, null)
        } finally {
            rateLimiterRegistry.onTaskFinished(queue.queueId)
        }
    }

    private suspend fun handleFailure(task: TaskInstance, queue: QueueSpec, errorReason: String, statusCode: Int?) {
        logger.warn("Cloud Task '${task.taskInstanceId}' failed. Reason: $errorReason. Applying retry policy.")
        val requeued = taskQueue.requeueWithBackoff(task, errorReason)
        runHistoryStore.saveTaskInstance(requeued)
        updateCloudTask(task.taskInstanceId, requeued.status, statusCode, null, errorReason)
    }

    private fun updateCloudTask(taskId: String, status: TaskStatus, responseCode: Int?, responseOutput: String?, error: String? = null) {
        val existing = taskStore.getTask(taskId) ?: return
        taskStore.saveTask(existing.copy(
            status = status,
            responseCode = responseCode,
            responseOutput = responseOutput,
            lastError = error,
            completedAtEpochMs = if (status == TaskStatus.COMPLETED) System.currentTimeMillis() else null
        ))
    }
}
