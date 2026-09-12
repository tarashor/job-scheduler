package com.tarashor.scheduler.cluster

import com.tarashor.scheduler.core.model.LeaderLease
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

interface LeaseStore {
    fun getCurrentLease(): LeaderLease?
    fun tryAcquire(nodeId: String, durationMs: Long): LeaderLease?
    fun tryRenew(nodeId: String, fencingToken: Long, durationMs: Long): LeaderLease?
    fun release(nodeId: String, fencingToken: Long): Boolean
}

class InMemoryLeaseStore : LeaseStore {
    private val current = AtomicReference<LeaderLease?>(null)
    private var tokenSequence = 0L

    @Synchronized
    override fun getCurrentLease(): LeaderLease? {
        val lease = current.get() ?: return null
        if (System.currentTimeMillis() > lease.expiresAtEpochMs) {
            return null // expired
        }
        return lease
    }

    @Synchronized
    override fun tryAcquire(nodeId: String, durationMs: Long): LeaderLease? {
        val now = System.currentTimeMillis()
        val existing = current.get()
        if (existing == null || now > existing.expiresAtEpochMs) {
            tokenSequence++
            val newLease = LeaderLease(
                leaderId = nodeId,
                fencingToken = tokenSequence,
                acquiredAtEpochMs = now,
                expiresAtEpochMs = now + durationMs
            )
            current.set(newLease)
            return newLease
        }
        return null
    }

    @Synchronized
    override fun tryRenew(nodeId: String, fencingToken: Long, durationMs: Long): LeaderLease? {
        val now = System.currentTimeMillis()
        val existing = current.get()
        if (existing != null && existing.leaderId == nodeId && existing.fencingToken == fencingToken && now <= existing.expiresAtEpochMs) {
            val renewed = existing.copy(expiresAtEpochMs = now + durationMs)
            current.set(renewed)
            return renewed
        }
        return null
    }

    @Synchronized
    override fun release(nodeId: String, fencingToken: Long): Boolean {
        val existing = current.get()
        if (existing != null && existing.leaderId == nodeId && existing.fencingToken == fencingToken) {
            current.set(null)
            return true
        }
        return false
    }
}

class LeaderElector(
    val nodeId: String,
    private val leaseStore: LeaseStore,
    private val leaseDurationMs: Long = 6_000,
    private val heartbeatIntervalMs: Long = 2_000,
    private val onElected: suspend (LeaderLease) -> Unit = {},
    private val onRevoked: suspend () -> Unit = {}
) {
    private val logger = LoggerFactory.getLogger("LeaderElector-$nodeId")
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val isRunning = AtomicBoolean(false)
    private val isLeader = AtomicBoolean(false)
    private var activeLease: LeaderLease? = null
    private var stepDownCooldownUntilEpochMs: Long = 0L

    fun isLeader(): Boolean = isLeader.get()
    fun getActiveLease(): LeaderLease? = activeLease

    fun start() {
        if (!isRunning.compareAndSet(false, true)) return
        logger.info("Starting leader elector for node: $nodeId")

        scope.launch {
            while (isRunning.get()) {
                try {
                    tick()
                } catch (e: CancellationException) {
                    break
                } catch (e: Exception) {
                    logger.error("Error during leader election tick", e)
                }
                delay(heartbeatIntervalMs)
            }
        }
    }

    private suspend fun tick() {
        val now = System.currentTimeMillis()
        if (isLeader.get()) {
            val lease = activeLease
            if (lease != null) {
                val renewed = leaseStore.tryRenew(nodeId, lease.fencingToken, leaseDurationMs)
                if (renewed != null) {
                    activeLease = renewed
                    logger.debug("Successfully renewed leader lease with fencing token: ${renewed.fencingToken}")
                } else {
                    logger.warn("Failed to renew lease. Stepping down from leadership.")
                    stepDownInternal()
                }
            } else {
                stepDownInternal()
            }
        } else {
            if (now < stepDownCooldownUntilEpochMs) {
                // In cooldown after manual step-down, let standby acquire
                return
            }
            // Attempt to acquire leadership if expired or unheld
            val acquired = leaseStore.tryAcquire(nodeId, leaseDurationMs)
            if (acquired != null) {
                logger.info("Node '$nodeId' successfully acquired leadership! FencingToken=${acquired.fencingToken}")
                activeLease = acquired
                isLeader.set(true)
                onElected(acquired)
            }
        }
    }

    suspend fun stepDown(cooldownMs: Long = 3_000L) {
        if (isLeader.get()) {
            logger.info("Manual step-down requested for leader '$nodeId' with cooldown ${cooldownMs}ms")
            stepDownCooldownUntilEpochMs = System.currentTimeMillis() + cooldownMs
            val lease = activeLease
            if (lease != null) {
                leaseStore.release(nodeId, lease.fencingToken)
            }
            stepDownInternal()
        }
    }

    private suspend fun stepDownInternal() {
        if (isLeader.compareAndSet(true, false)) {
            activeLease = null
            onRevoked()
        }
    }

    suspend fun stop() {
        if (isRunning.compareAndSet(true, false)) {
            logger.info("Stopping leader elector for node: $nodeId")
            if (isLeader.get()) {
                stepDown()
            }
            scope.cancel()
        }
    }
}
