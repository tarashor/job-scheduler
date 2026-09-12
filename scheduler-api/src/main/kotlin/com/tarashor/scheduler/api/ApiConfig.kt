package com.tarashor.scheduler.api

import com.tarashor.scheduler.cluster.LeaseStore
import com.tarashor.scheduler.queue.TaskQueue
import com.tarashor.scheduler.storage.*
import kotlinx.serialization.json.Json
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.converter.HttpMessageConverter
import org.springframework.http.converter.json.KotlinSerializationJsonHttpMessageConverter
import org.springframework.web.servlet.config.annotation.CorsRegistry
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer

@Configuration
class ApiConfig : WebMvcConfigurer {

    private val storageBundle: StorageBundle by lazy {
        StorageFactory.createApiStorageFromEnv()
    }

    @Bean
    fun jobMetadataStore(): JobMetadataStore = storageBundle.jobMetadataStore

    @Bean
    fun runHistoryStore(): RunHistoryStore = storageBundle.runHistoryStore

    @Bean
    fun workerRegistry(): WorkerRegistry = storageBundle.workerRegistry

    @Bean
    fun taskQueue(): TaskQueue = storageBundle.taskQueue

    @Bean
    fun leaseStore(): LeaseStore = storageBundle.leaseStore

    @Bean
    fun schedulerStorage(
        jobMetadataStore: JobMetadataStore,
        runHistoryStore: RunHistoryStore,
        workerRegistry: WorkerRegistry
    ): SchedulerStorage = CompositeSchedulerStorage(jobMetadataStore, runHistoryStore, workerRegistry)

    @Bean
    fun jobTriggerService(): com.tarashor.scheduler.service.JobTriggerService = storageBundle.triggerService

    override fun configureMessageConverters(converters: MutableList<HttpMessageConverter<*>>) {
        val json = Json {
            prettyPrint = true
            isLenient = true
            ignoreUnknownKeys = true
            encodeDefaults = true
        }
        converters.add(0, KotlinSerializationJsonHttpMessageConverter(json))
    }

    override fun addCorsMappings(registry: CorsRegistry) {
        registry.addMapping("/**")
            .allowedOriginPatterns("*")
            .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
            .allowedHeaders("*")
    }
}
