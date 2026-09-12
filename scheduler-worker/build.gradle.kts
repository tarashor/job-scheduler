plugins {
    application
}

application {
    mainClass.set("com.tarashor.scheduler.worker.WorkerAppKt")
}

dependencies {
    implementation(project(":scheduler-common"))
    implementation(project(":scheduler-storage"))
    val ktorVersion = "3.1.1"
    implementation("io.ktor:ktor-client-core:$ktorVersion")
    implementation("io.ktor:ktor-client-cio:$ktorVersion")
    implementation("io.ktor:ktor-client-content-negotiation:$ktorVersion")
}
