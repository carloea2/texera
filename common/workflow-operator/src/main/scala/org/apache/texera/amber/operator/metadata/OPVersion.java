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

package org.apache.texera.amber.operator.metadata;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class OPVersion {
    private static Git git = null;
    private static Map<String, String> opMap = new HashMap<>();
    static {
        try {
            File texeraHome = Path.of(System.getenv().getOrDefault("TEXERA_HOME", ".")).toFile();
            Repository repository = new FileRepositoryBuilder()
                    .readEnvironment()
                    .findGitDir(texeraHome)
                    .build();
            git = new Git(repository);
        } catch (IOException e) {
            git = null;
        }
    }

    public static String getVersion(String operatorName, String operatorPath) {
        if(!opMap.containsKey(operatorName)) {
            if (git == null) {
                opMap.put(operatorName, "N/A");
                return opMap.get(operatorName);
            }
            try {
                String version = git.log().addPath(operatorPath).setMaxCount(1).call().iterator().next().getName();
                opMap.put(operatorName, version);
            } catch (GitAPIException e) {
                opMap.put(operatorName, "N/A");
            } catch (NullPointerException | NoSuchElementException e) {
                opMap.put(operatorName, "N/A");
            }
        }
        return opMap.get(operatorName);
    }

}
