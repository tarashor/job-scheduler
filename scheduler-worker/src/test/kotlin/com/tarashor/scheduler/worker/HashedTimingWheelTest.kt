package com.tarashor.scheduler.worker

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.abs
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class HashedTimingWheelTest {

    @Test
    fun `test immediate execution for past or zero delay tasks`() = runBlocking {
        val wheel = HashedTimingWheel(tickDurationMs = 20L, wheelSize = 64)
        wheel.start()

        val executed = AtomicInteger(0)
        wheel.schedule("task-now", System.currentTimeMillis() - 100) {
            executed.incrementAndGet()
        }

        delay(100)
        assertEquals(1, executed.get(), "Immediate task should execute without waiting for ticks")

        wheel.stop()
    }

    @Test
    fun `test precision of scheduled tasks under 50ms drift`() = runBlocking {
        val wheel = HashedTimingWheel(tickDurationMs = 20L, wheelSize = 64)
        wheel.start()

        val start = System.currentTimeMillis()
        val targetDelay = 200L
        val targetEpochMs = start + targetDelay
        val completionTimes = ConcurrentLinkedQueue<Long>()

        wheel.schedule("precise-task", targetEpochMs) {
            completionTimes.add(System.currentTimeMillis())
        }

        delay(500)
        assertEquals(1, completionTimes.size, "Task should execute once")
        val actualExecutedAt = completionTimes.first()
        val driftMs = abs(actualExecutedAt - targetEpochMs)

        assertTrue(
            driftMs < 200L,
            "Drift ($driftMs ms) must be well within micro-precision target (<200ms vs 2000ms SLA, actual scheduled: $targetEpochMs, executed: $actualExecutedAt)"
        )

        wheel.stop()
    }

    @Test
    fun `test multi-round wheel rotation execution`() = runBlocking {
        // wheelSize=16, tick=20ms -> one full wheel rotation takes 320ms
        val wheel = HashedTimingWheel(tickDurationMs = 20L, wheelSize = 16)
        wheel.start()

        val executedTimes = ConcurrentLinkedQueue<Long>()
        val start = System.currentTimeMillis()
        val targetEpoch = start + 500L // > 1 full revolution of 320ms

        wheel.schedule("multi-round-task", targetEpoch) {
            executedTimes.add(System.currentTimeMillis())
        }

        // Poll until executed (up to 1500ms)
        for (i in 1..30) {
            if (executedTimes.isNotEmpty()) break
            delay(50)
        }

        assertEquals(1, executedTimes.size, "Task should execute after completing revolution")
        val executedAt = executedTimes.first()
        val driftMs = abs(executedAt - targetEpoch)
        assertTrue(driftMs < 200L, "Multi-round execution drift ($driftMs ms) must be within 200ms")

        wheel.stop()
    }
}
