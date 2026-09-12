package com.tarashor.scheduler.storage

import com.tarashor.scheduler.core.model.OutboxEvent

/**
 * Domain Port for transactional outbox event persistence and guaranteed at-least-once dispatch.
 */
interface OutboxStore {
    fun saveOutboxEvent(event: OutboxEvent)
    fun fetchPendingOutboxEvents(limit: Int = 100): List<OutboxEvent>
    fun markOutboxDispatched(eventId: String, dispatchedAtEpochMs: Long = System.currentTimeMillis())
}
