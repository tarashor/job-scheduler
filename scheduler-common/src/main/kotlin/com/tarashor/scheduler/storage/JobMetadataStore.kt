package com.tarashor.scheduler.storage

import com.tarashor.scheduler.core.model.JobSpec

/**
 * Domain Port for managing persistent Job specifications.
 * Owned by scheduler-api microservice in a Database-per-Microservice architecture.
 */
interface JobMetadataStore {
    fun saveJob(job: JobSpec)
    fun getJob(jobId: String): JobSpec?
    fun listJobs(): List<JobSpec>
    fun deleteJob(jobId: String): Boolean
}
