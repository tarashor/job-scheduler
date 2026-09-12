plugins {
    id("org.springframework.boot")
    id("io.spring.dependency-management")
    kotlin("plugin.spring")
}

dependencies {
    implementation(project(":scheduler-common"))
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
    implementation(enforcedPlatform("org.jetbrains.kotlinx:kotlinx-serialization-bom:1.8.0"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

