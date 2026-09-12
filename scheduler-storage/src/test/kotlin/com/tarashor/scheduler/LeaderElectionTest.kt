package com.tarashor.scheduler

import com.tarashor.scheduler.cluster.InMemoryLeaseStore
import com.tarashor.scheduler.cluster.LeaderElector
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class LeaderElectionTest {

    @Test
    fun `test lease acquisition and mutual exclusion`() {
        val store = InMemoryLeaseStore()

        val lease1 = store.tryAcquire("master-1", 5000)
        assertNotNull(lease1)
        assertEquals("master-1", lease1.leaderId)
        assertEquals(1L, lease1.fencingToken)

        // Second candidate tries to acquire immediately -> should be rejected
        val lease2 = store.tryAcquire("master-2", 5000)
        assertEquals(null, lease2)

        // master-1 can renew its lease
        val renewed = store.tryRenew("master-1", lease1.fencingToken, 5000)
        assertNotNull(renewed)
        assertEquals(1L, renewed.fencingToken)

        // master-1 releases lease
        val released = store.release("master-1", lease1.fencingToken)
        assertTrue(released)

        // Now master-2 acquires with incremented fencing token
        val lease2AfterRelease = store.tryAcquire("master-2", 5000)
        assertNotNull(lease2AfterRelease)
        assertEquals("master-2", lease2AfterRelease.leaderId)
        assertEquals(2L, lease2AfterRelease.fencingToken)
    }

    @Test
    fun `test automated leader failover between electors`() = runBlocking {
        val store = InMemoryLeaseStore()

        val elector1 = LeaderElector("node-1", store, leaseDurationMs = 1500, heartbeatIntervalMs = 400)
        val elector2 = LeaderElector("node-2", store, leaseDurationMs = 1500, heartbeatIntervalMs = 400)

        elector1.start()
        delay(600)

        assertTrue(elector1.isLeader(), "Node 1 should be elected leader")
        assertFalse(elector2.isLeader(), "Node 2 should not be leader")

        elector2.start()
        delay(600)
        assertTrue(elector1.isLeader(), "Node 1 should still be leader")
        assertFalse(elector2.isLeader(), "Node 2 should still be standby")

        // Trigger failover by stepping down node 1
        elector1.stepDown()
        delay(800)

        assertFalse(elector1.isLeader(), "Node 1 should have stepped down")
        assertTrue(elector2.isLeader(), "Node 2 should have taken over leadership")
        assertEquals(2L, elector2.getActiveLease()?.fencingToken, "Fencing token should be incremented to 2")

        elector1.stop()
        elector2.stop()
    }
}
