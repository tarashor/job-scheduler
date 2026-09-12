plugins {
    application
}

application {
    mainClass.set("com.tarashor.scheduler.api.ApiAppKt")
}

dependencies {
    implementation(project(":scheduler-common"))
    implementation(project(":scheduler-storage"))
    val ktorVersion = "3.1.1"
    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("io.ktor:ktor-server-cio:$ktorVersion")
    implementation("io.ktor:ktor-server-content-negotiation:$ktorVersion")
    implementation("io.ktor:ktor-serialization-kotlinx-json:$ktorVersion")
    implementation("io.ktor:ktor-server-cors:$ktorVersion")
}
