
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.bundling.Jar

plugins {
    id("polaris-java")
    antlr                // Gradle's built-in ANTLR plugin — no version, no alias needed
}

val antlrOutputDir = layout.buildDirectory.dir("generated-src/antlr/main")

// ── Correct Kotlin DSL way to configure the ANTLR task ──────────────────────
tasks.named<AntlrTask>("generateGrammarSource") {
    maxHeapSize = "64m"
    arguments  = listOf("-visitor", "-no-listener", "-package", "org.apache.polaris.sql.grammar")
    outputDirectory = antlrOutputDir.get().asFile
}

sourceSets {
    main {
        java { srcDir(antlrOutputDir) }
    }
}

tasks.named<JavaCompile>("compileJava") {
    dependsOn("generateGrammarSource")
}

dependencies {
    // ANTLR tool (code-gen only) + its runtime (shipped in the jar)
    antlr(libs.antlr4)
    implementation(libs.antlr4.engine.runtime)

    // Iceberg
    implementation(platform(libs.iceberg.bom))
    implementation("org.apache.iceberg:iceberg-api")
    implementation("org.apache.iceberg:iceberg-core")
    implementation("org.apache.iceberg:iceberg-data")

    // Polaris catalog layer
    implementation(project(":polaris-core"))

    implementation(libs.guava)
    implementation(libs.slf4j.api)
}