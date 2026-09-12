package com.tarashor.scheduler.worker

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import kotlin.test.assertNotNull

@SpringBootTest
class WorkerApplicationTest {

    @Autowired
    private lateinit var workerNode: WorkerNode

    @Test
    fun `contextLoads and worker node bean is injected`() {
        assertNotNull(workerNode)
        assertNotNull(workerNode.workerId)
    }
}
