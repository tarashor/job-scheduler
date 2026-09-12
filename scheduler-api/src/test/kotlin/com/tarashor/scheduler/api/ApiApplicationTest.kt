package com.tarashor.scheduler.api

import com.tarashor.scheduler.core.model.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.*

@SpringBootTest
@AutoConfigureMockMvc
class ApiApplicationTest {

    @Autowired
    private lateinit var mockMvc: MockMvc

    @Test
    fun `contextLoads and health endpoint responds`() {
        mockMvc.perform(get("/api/health"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.queueSize").isNumber)
            .andExpect(jsonPath("$.dlqSize").isNumber)
    }

    @Test
    fun `test UI dashboard endpoint returns HTML`() {
        mockMvc.perform(get("/"))
            .andExpect(status().isOk)
            .andExpect(content().contentTypeCompatibleWith(MediaType.TEXT_HTML))
    }

    @Test
    fun `test create and list jobs via Spring MVC`() {
        val jsonPayload = """
            {
              "jobId": "spring-test-job",
              "name": "Spring Boot Test Job",
              "schedule": {
                "type": "Immediate"
              },
              "tasks": [
                {
                  "taskId": "task-1",
                  "name": "Task 1",
                  "action": {
                    "type": "Shell",
                    "command": "echo 'Hello from Spring Boot'"
                  }
                }
              ]
            }
        """.trimIndent()

        mockMvc.perform(
            post("/api/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content(jsonPayload)
        )
            .andExpect(status().isCreated)
            .andExpect(jsonPath("$.jobId").value("spring-test-job"))

        mockMvc.perform(get("/api/jobs"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$[?(@.jobId == 'spring-test-job')]").exists())
    }

    @Test
    fun `test Cloud Tasks queues and tasks REST endpoints`() {
        // 1. Create queue with rate limits
        val queuePayload = """
            {
              "queueId": "email-notifications",
              "rateLimits": {
                "maxDispatchesPerSecond": 15.0,
                "maxConcurrentDispatches": 3
              },
              "retryConfig": {
                "maxAttempts": 4
              }
            }
        """.trimIndent()

        mockMvc.perform(
            post("/api/queues")
                .contentType(MediaType.APPLICATION_JSON)
                .content(queuePayload)
        )
            .andExpect(status().isCreated)
            .andExpect(jsonPath("$.queueId").value("email-notifications"))
            .andExpect(jsonPath("$.rateLimits.maxDispatchesPerSecond").value(15.0))

        // 2. List queues
        mockMvc.perform(get("/api/queues"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$[?(@.queueId == 'email-notifications')]").exists())

        // 3. Create task with HTTP target
        val taskPayload = """
            {
              "taskId": "welcome-email-user-123",
              "target": {
                "type": "HttpRequest",
                "url": "http://localhost:8080/api/mock/target",
                "httpMethod": "POST",
                "body": "{\"userId\": 123, \"template\": \"welcome\"}"
              }
            }
        """.trimIndent()

        mockMvc.perform(
            post("/api/queues/email-notifications/tasks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(taskPayload)
        )
            .andExpect(status().isCreated)
            .andExpect(jsonPath("$.taskId").value("welcome-email-user-123"))
            .andExpect(jsonPath("$.queueId").value("email-notifications"))
            .andExpect(jsonPath("$.status").value("QUEUED"))

        // 4. Pause queue
        mockMvc.perform(post("/api/queues/email-notifications/pause"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.state").value("PAUSED"))

        // 5. Resume queue
        mockMvc.perform(post("/api/queues/email-notifications/resume"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.state").value("RUNNING"))

        // 6. Test Mock Target
        mockMvc.perform(
            post("/api/mock/target")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"event\": \"ping\"}")
        )
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.status").value("SUCCESS"))
    }
}

