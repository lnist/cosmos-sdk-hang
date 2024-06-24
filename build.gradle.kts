plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.wiremock:wiremock:3.5.4")
    testImplementation("com.azure:azure-cosmos:4.61.1")
}

tasks.test {
    useJUnitPlatform()
}