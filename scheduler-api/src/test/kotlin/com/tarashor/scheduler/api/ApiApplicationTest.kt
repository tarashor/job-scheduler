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
}
