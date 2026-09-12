plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}
rootProject.name = "job-scheduler"

include("scheduler-common")
include("scheduler-storage")
include("scheduler-coordinator")
include("scheduler-worker")
include("scheduler-api")