package com.tarashor.scheduler

import com.tarashor.scheduler.core.model.QueueSpec
import com.tarashor.scheduler.core.model.QueueState
import com.tarashor.scheduler.core.model.RateLimits
import com.tarashor.scheduler.core.ratelimit.QueueRateLimiterRegistry
import com.tarashor.scheduler.core.ratelimit.TokenBucket
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class QueueRateLimiterTest {

    @Test
    fun `test TokenBucket rate limit consumption and refill`() {
        // Capacity 2, rate 1 token per second
        val bucket = TokenBucket(ratePerSecond = 1.0, capacity = 2)

        assertTrue(bucket.tryAcquire(1.0), "First token acquired")
        assertTrue(bucket.tryAcquire(1.0), "Second token acquired")
        assertFalse(bucket.tryAcquire(1.0), "Third token rejected (exhausted)")

        // Sleep 1100ms to replenish 1 token
        Thread.sleep(1100)
        assertTrue(bucket.tryAcquire(1.0), "Replenished token acquired")
    }

    @Test
    fun `test QueueRateLimiterRegistry concurrency and pause enforcement`() {
        val registry = QueueRateLimiterRegistry()
        val queue = QueueSpec(
            queueId = "rate-limited-q",
            state = QueueState.RUNNING,
            rateLimits = RateLimits(maxDispatchesPerSecond = 100.0, maxConcurrentDispatches = 2, maxBurstSize = 10)
        )

        // Dispatch 2 tasks
        assertTrue(registry.canDispatch(queue))
        registry.onTaskDispatched("rate-limited-q")

        assertTrue(registry.canDispatch(queue))
        registry.onTaskDispatched("rate-limited-q")

        // Max concurrency reached (2 active)
        assertFalse(registry.canDispatch(queue), "Should reject when concurrency limit reached")
        assertEquals(2, registry.getActiveCount("rate-limited-q"))

        // Complete 1 task
        registry.onTaskFinished("rate-limited-q")
        assertEquals(1, registry.getActiveCount("rate-limited-q"))
        assertTrue(registry.canDispatch(queue), "Can dispatch after capacity freed")

        // Pausing queue halts all dispatches immediately
        val pausedQueue = queue.copy(state = QueueState.PAUSED)
        assertFalse(registry.canDispatch(pausedQueue), "Paused queue must not dispatch tasks")
    }
}
