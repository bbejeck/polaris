
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

plugins {
    id("polaris-java")
}

// Isolated configuration: ANTLR 4 tool jar, does not leak into compile/runtime
val antlrTool: Configuration by configurations.creating

val antlrOutputDir = layout.buildDirectory.dir("generated-src/antlr/main")
val antlrPackageDir = layout.buildDirectory.dir("generated-src/antlr/main/org/apache/polaris/sql/grammar")
val grammarFile    = file("src/main/antlr/IcebergSQL.g4")

val generateGrammarSource by tasks.registering(JavaExec::class) {
    description = "Generate Java sources from IcebergSQL.g4 using ANTLR 4"
    group       = "build"

    classpath  = antlrTool
    mainClass  = "org.antlr.v4.Tool"

    doFirst { antlrPackageDir.get().asFile.mkdirs() }

    args = listOf(
        "-visitor",
        "-no-listener",
        "-package", "org.apache.polaris.sql.grammar",
        "-o", antlrPackageDir.get().asFile.absolutePath,  // output into full package path
        grammarFile.absolutePath
    )

    inputs.file(grammarFile)
    outputs.dir(antlrOutputDir)  // declare root as output for incremental build tracking
}

sourceSets {
    main {
        java { srcDir(antlrOutputDir) }  // root — Java compiler walks subdirs automatically
    }
}

dependencies {
    antlrTool(libs.antlr4)                      // ANTLR 4 tool — code-gen only, not shipped
    implementation(libs.antlr4.engine.runtime)  // ANTLR 4 runtime — shipped in our jar

    implementation(platform(libs.iceberg.bom))
    implementation("org.apache.iceberg:iceberg-api")
    implementation("org.apache.iceberg:iceberg-core")
    implementation("org.apache.iceberg:iceberg-data")
    implementation("org.apache.iceberg:iceberg-parquet")

    implementation(project(":polaris-core"))
    implementation(libs.guava)
    implementation(libs.slf4j.api)

    testImplementation(libs.mockito.junit.jupiter)

    testImplementation("org.apache.iceberg:iceberg-aws")
    testImplementation("org.apache.parquet:parquet-column:1.16.0")
    testImplementation(libs.hadoop.common)
    testImplementation(libs.hadoop.client.runtime)

    testImplementation(platform(libs.testcontainers.bom))
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")

    testImplementation(project(":polaris-minio-testcontainer"))
}