package com.tarashor.scheduler.coordinator

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class CoordinatorApplication

fun main(args: Array<String>) {
    runApplication<CoordinatorApplication>(*args)
}
