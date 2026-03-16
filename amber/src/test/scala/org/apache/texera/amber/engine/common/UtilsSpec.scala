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

package org.apache.texera.amber.engine.common

import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, FileVisitor, FileSystems, Files, Path}

class UtilsSpec extends AnyFlatSpec {

  "resolveAmberHomePath" should "prefer an amber directory under the current working directory" in
    withTempDirectory { tempDirectory =>
      val preferredRepo = Files.createDirectory(tempDirectory.resolve("preferred-repo"))
      val siblingRepo = Files.createDirectory(tempDirectory.resolve("sibling-repo"))
      val preferredAmber = Files.createDirectories(preferredRepo.resolve("amber"))
      Files.createDirectories(siblingRepo.resolve("amber"))

      assert(Utils.resolveAmberHomePath(preferredRepo) == preferredAmber.toRealPath())
    }

  it should "fall back to a sibling amber directory when the current working directory has none" in
    withTempDirectory { tempDirectory =>
      val repoRoot = Files.createDirectory(tempDirectory.resolve("repo-root"))
      val moduleDirectory = Files.createDirectory(repoRoot.resolve("module"))
      val amberDirectory = Files.createDirectories(repoRoot.resolve("amber"))

      assert(Utils.resolveAmberHomePath(moduleDirectory) == amberDirectory.toRealPath())
    }

  it should "use amber-specific wording when searching from a filesystem root" in {
    val filesystemRoot = FileSystems.getDefault.getRootDirectories.iterator().next().toRealPath()
    val exception = intercept[RuntimeException] {
      Utils.resolveAmberHomePath(filesystemRoot)
    }

    assert(exception.getMessage.contains("amber home"))
  }

  private def withTempDirectory(test: Path => Any): Unit = {
    val tempDirectory = Files.createTempDirectory("utils-spec-")
    try {
      test(tempDirectory.toRealPath())
    } finally {
      deleteRecursively(tempDirectory)
    }
  }

  private def deleteRecursively(path: Path): Unit = {
    if (!Files.exists(path)) {
      return
    }

    Files.walkFileTree(
      path,
      new FileVisitor[Path] {
        override def preVisitDirectory(
            directory: Path,
            attributes: BasicFileAttributes
        ): FileVisitResult = FileVisitResult.CONTINUE

        override def visitFile(file: Path, attributes: BasicFileAttributes): FileVisitResult = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }

        override def visitFileFailed(file: Path, exception: java.io.IOException): FileVisitResult =
          throw exception

        override def postVisitDirectory(
            directory: Path,
            exception: java.io.IOException
        ): FileVisitResult = {
          Option(exception).foreach(throw _)
          Files.delete(directory)
          FileVisitResult.CONTINUE
        }
      }
    )
  }
}
