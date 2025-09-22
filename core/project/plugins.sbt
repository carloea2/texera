// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.11.0")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.1"
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.11.1")
// for scalapb code gen
addSbtPlugin("org.typelevel" % "sbt-fs2-grpc" % "2.5.0")

// JOOQ dependencies for code generation
libraryDependencies ++= Seq(
  "org.jooq" % "jooq-codegen" % "3.16.23",
  "com.typesafe" % "config" % "1.4.3",
  "org.postgresql" % "postgresql" % "42.7.4"
)