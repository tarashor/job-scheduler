package com.tarashor.scheduler.api

import com.tarashor.scheduler.core.model.*
import com.tarashor.scheduler.service.CloudTaskService
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

/**
 * REST API Gateway Controller adhering to Google Cloud Tasks specification:
 * - Queues API: manage queues, pause/resume, purge, rate limits
 * - Tasks API: create delayed or immediate tasks, force-run, cancel
 * - Mock Webhook Target: for local testing of HTTP push tasks
 */
@RestController
@RequestMapping("/api")
class CloudTaskController(
    private val cloudTaskService: CloudTaskService
) {
    private val logger = LoggerFactory.getLogger(CloudTaskController::class.java)

    // ==========================================
    // Google Cloud Tasks: Queues Endpoints
    // ==========================================

    @GetMapping("/queues")
    fun listQueues(): List<QueueStats> {
        val queues = cloudTaskService.listQueues()
        return queues.map { cloudTaskService.getQueueStats(it.queueId) }
    }

    @PostMapping("/queues")
    fun createQueue(@RequestBody queue: QueueSpec): ResponseEntity<QueueSpec> {
        val created = cloudTaskService.createQueue(queue)
        return ResponseEntity.status(HttpStatus.CREATED).body(created)
    }

    @GetMapping("/queues/{queueId}")
    fun getQueue(@PathVariable queueId: String): ResponseEntity<QueueStats> {
        val queue = cloudTaskService.getQueue(queueId) ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(cloudTaskService.getQueueStats(queueId))
    }

    @DeleteMapping("/queues/{queueId}")
    fun deleteQueue(@PathVariable queueId: String): ResponseEntity<Void> {
        val deleted = cloudTaskService.deleteQueue(queueId)
        return if (deleted) ResponseEntity.ok().build() else ResponseEntity.notFound().build()
    }

    @PostMapping("/queues/{queueId}/pause")
    fun pauseQueue(@PathVariable queueId: String): ResponseEntity<QueueSpec> {
        return try {
            ResponseEntity.ok(cloudTaskService.pauseQueue(queueId))
        } catch (e: Exception) {
            ResponseEntity.notFound().build()
        }
    }

    @PostMapping("/queues/{queueId}/resume")
    fun resumeQueue(@PathVariable queueId: String): ResponseEntity<QueueSpec> {
        return try {
            ResponseEntity.ok(cloudTaskService.resumeQueue(queueId))
        } catch (e: Exception) {
            ResponseEntity.notFound().build()
        }
    }

    @PostMapping("/queues/{queueId}/purge")
    fun purgeQueue(@PathVariable queueId: String): ResponseEntity<Map<String, Any>> {
        val count = cloudTaskService.purgeQueue(queueId)
        return ResponseEntity.ok(mapOf("purgedTaskCount" to count, "queueId" to queueId))
    }

    // ==========================================
    // Google Cloud Tasks: Tasks Endpoints
    // ==========================================

    @GetMapping("/queues/{queueId}/tasks")
    fun listTasks(
        @PathVariable queueId: String,
        @RequestParam(required = false) status: TaskStatus?
    ): List<TaskSpec> {
        return cloudTaskService.listTasks(queueId = queueId, status = status)
    }

    @PostMapping("/queues/{queueId}/tasks")
    fun createTask(
        @PathVariable queueId: String,
        @RequestBody task: TaskSpec
    ): ResponseEntity<TaskSpec> = runBlocking {
        try {
            val created = cloudTaskService.createTask(queueId, task)
            ResponseEntity.status(HttpStatus.CREATED).body(created)
        } catch (e: Exception) {
            logger.error("Error creating task in queue '$queueId'", e)
            ResponseEntity.badRequest().build()
        }
    }

    @GetMapping("/queues/{queueId}/tasks/{taskId}")
    fun getTask(
        @PathVariable queueId: String,
        @PathVariable taskId: String
    ): ResponseEntity<TaskSpec> {
        val task = cloudTaskService.getTask(queueId, taskId) ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(task)
    }

    @DeleteMapping("/queues/{queueId}/tasks/{taskId}")
    fun deleteTask(
        @PathVariable queueId: String,
        @PathVariable taskId: String
    ): ResponseEntity<Void> = runBlocking {
        val deleted = cloudTaskService.deleteTask(queueId, taskId)
        if (deleted) ResponseEntity.ok().build() else ResponseEntity.notFound().build()
    }

    @PostMapping("/queues/{queueId}/tasks/{taskId}/run")
    fun runTask(
        @PathVariable queueId: String,
        @PathVariable taskId: String
    ): ResponseEntity<TaskSpec> = runBlocking {
        try {
            val forced = cloudTaskService.runTask(queueId, taskId)
            ResponseEntity.ok(forced)
        } catch (e: Exception) {
            ResponseEntity.notFound().build()
        }
    }

    // ==========================================
    // Mock Target Endpoint for Testing Cloud Tasks HTTP Webhooks
    // ==========================================

    @PostMapping("/mock/target")
    fun mockTargetPost(request: jakarta.servlet.http.HttpServletRequest): ResponseEntity<Map<String, Any>> {
        val body = request.reader.readText()
        logger.info("Mock Target received HTTP POST with body: $body")
        return ResponseEntity.ok(mapOf(
            "status" to "SUCCESS",
            "message" to "Cloud Task executed successfully by mock target",
            "echoPayload" to body,
            "timestampEpochMs" to System.currentTimeMillis()
        ))
    }


    @GetMapping("/mock/target")
    fun mockTargetGet(): ResponseEntity<Map<String, String>> {
        return ResponseEntity.ok(mapOf("status" to "OK", "service" to "CloudTasksMockTarget"))
    }
}
