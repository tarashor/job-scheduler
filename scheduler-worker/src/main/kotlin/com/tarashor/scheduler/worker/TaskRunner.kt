package com.tarashor.scheduler.worker

import com.tarashor.scheduler.core.model.JobAction
import com.tarashor.scheduler.core.model.TaskExecutionResult
import com.tarashor.scheduler.core.model.TaskInstance
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.TimeUnit

interface TaskRunner {
    suspend fun execute(instance: TaskInstance, timeoutMs: Long): TaskExecutionResult
}

typealias JobRunner = TaskRunner

class DefaultTaskRunner : TaskRunner {
    private val logger = LoggerFactory.getLogger(DefaultTaskRunner::class.java)
    private val httpClient: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build()

    override suspend fun execute(instance: TaskInstance, timeoutMs: Long): TaskExecutionResult {
        return try {
            withTimeout(timeoutMs) {
                when (val action = instance.action) {
                    is JobAction.Shell -> executeShell(instance, action)
                    is JobAction.Http -> executeHttp(instance, action, timeoutMs)
                    is JobAction.Simulate -> executeSimulate(instance, action)
                }
            }
        } catch (e: TimeoutCancellationException) {
            logger.warn("Task '${instance.taskInstanceId}' timed out after ${timeoutMs}ms")
            TaskExecutionResult(
                taskInstanceId = instance.taskInstanceId,
                success = false,
                error = "Task timed out after ${timeoutMs}ms"
            )
        } catch (e: Exception) {
            logger.error("Error executing task '${instance.taskInstanceId}'", e)
            TaskExecutionResult(
                taskInstanceId = instance.taskInstanceId,
                success = false,
                error = e.message ?: e.javaClass.simpleName
            )
        }
    }

    private suspend fun executeShell(instance: TaskInstance, action: JobAction.Shell): TaskExecutionResult =
        withContext(Dispatchers.IO) {
            logger.info("Executing shell command for '${instance.taskInstanceId}': ${action.command}")
            val process = ProcessBuilder("/bin/sh", "-c", action.command)
                .redirectErrorStream(true)
                .start()

            val output = process.inputStream.bufferedReader().readText()
            val finished = process.waitFor(60, TimeUnit.SECONDS)
            if (!finished) {
                process.destroyForcibly()
                return@withContext TaskExecutionResult(
                    taskInstanceId = instance.taskInstanceId,
                    success = false,
                    error = "Process forcibly terminated after 60s"
                )
            }

            val exitCode = process.exitValue()
            TaskExecutionResult(
                taskInstanceId = instance.taskInstanceId,
                success = exitCode == 0,
                output = output.trim(),
                error = if (exitCode != 0) "Process exited with code $exitCode" else null
            )
        }

    private suspend fun executeHttp(instance: TaskInstance, action: JobAction.Http, timeoutMs: Long): TaskExecutionResult =
        withContext(Dispatchers.IO) {
            logger.info("Executing HTTP ${action.method} request for '${instance.taskInstanceId}' to ${action.url}")
            try {
                val requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(action.url))
                    .timeout(Duration.ofMillis(timeoutMs))

                action.headers.forEach { (k, v) -> requestBuilder.header(k, v) }

                val bodyPublisher = if (action.body != null) {
                    HttpRequest.BodyPublishers.ofString(action.body)
                } else {
                    HttpRequest.BodyPublishers.noBody()
                }

                requestBuilder.method(action.method, bodyPublisher)

                val response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString())
                val isSuccess = response.statusCode() in 200..299
                val body = response.body() ?: ""
                TaskExecutionResult(
                    taskInstanceId = instance.taskInstanceId,
                    success = isSuccess,
                    output = "HTTP ${response.statusCode()}: $body",
                    error = if (!isSuccess) "HTTP status ${response.statusCode()}" else null
                )
            } catch (e: Exception) {
                TaskExecutionResult(
                    taskInstanceId = instance.taskInstanceId,
                    success = false,
                    error = e.message ?: e.javaClass.simpleName
                )
            }
        }

    private suspend fun executeSimulate(instance: TaskInstance, action: JobAction.Simulate): TaskExecutionResult {
        logger.info("Simulating task '${instance.taskInstanceId}' for ${action.durationMs}ms")
        delay(action.durationMs)
        return if (action.shouldFail) {
            TaskExecutionResult(
                taskInstanceId = instance.taskInstanceId,
                success = false,
                error = "Simulated failure: ${action.message}"
            )
        } else {
            TaskExecutionResult(
                taskInstanceId = instance.taskInstanceId,
                success = true,
                output = "Simulated success: ${action.message}"
            )
        }
    }
}
