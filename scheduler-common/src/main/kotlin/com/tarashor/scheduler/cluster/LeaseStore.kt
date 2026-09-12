package com.tarashor.scheduler.cluster

import com.tarashor.scheduler.core.model.LeaderLease
import java.util.concurrent.atomic.AtomicReference

interface DistributedLockStore {
    fun tryAcquireLock(key: String, durationMs: Long): Boolean
    fun releaseLock(key: String): Boolean
}

interface LeaseStore : DistributedLockStore {
    fun getCurrentLease(): LeaderLease?
    fun tryAcquire(nodeId: String, durationMs: Long): LeaderLease?
    fun tryRenew(nodeId: String, fencingToken: Long, durationMs: Long): LeaderLease?
    fun release(nodeId: String, fencingToken: Long): Boolean
}

class InMemoryLeaseStore : LeaseStore {
    private val current = AtomicReference<LeaderLease?>(null)
    private var tokenSequence = 0L
    private val locks = java.util.concurrent.ConcurrentHashMap<String, Long>()

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

    @Synchronized
    override fun tryAcquireLock(key: String, durationMs: Long): Boolean {
        val now = System.currentTimeMillis()
        val expiry = locks[key]
        if (expiry == null || now > expiry) {
            locks[key] = now + durationMs
            return true
        }
        return false
    }

    @Synchronized
    override fun releaseLock(key: String): Boolean {
        return locks.remove(key) != null
    }
}
