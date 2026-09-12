plugins {
    application
}

application {
    mainClass.set("com.tarashor.scheduler.coordinator.CoordinatorAppKt")
}

dependencies {
    implementation(project(":scheduler-common"))
    implementation(project(":scheduler-storage"))
    testImplementation(project(":scheduler-worker"))
}
