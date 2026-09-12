package com.tarashor.scheduler.storage

import com.tarashor.scheduler.core.model.WorkerInfo

/**
 * Domain Port for tracking worker node health, capacity, and active execution heartbeats.
 */
interface WorkerRegistry {
    fun upsertWorker(worker: WorkerInfo)
    fun getWorker(workerId: String): WorkerInfo?
    fun listWorkers(): List<WorkerInfo>
}
