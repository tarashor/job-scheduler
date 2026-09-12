dependencies {
    implementation(project(":scheduler-common"))
    implementation("org.xerial:sqlite-jdbc:3.49.1.0")
    implementation("org.postgresql:postgresql:42.7.5")
    implementation("com.zaxxer:HikariCP:6.2.1")
    implementation("redis.clients:jedis:5.2.0")
}
