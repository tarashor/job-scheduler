package com.tarashor.scheduler.coordinator

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import kotlin.test.assertNotNull

@SpringBootTest
class CoordinatorApplicationTest {

    @Autowired
    private lateinit var coordinator: SchedulerCoordinator

    @Test
    fun `contextLoads and coordinator bean is injected`() {
        assertNotNull(coordinator)
        assertNotNull(coordinator.coordinatorId)
    }
}
