/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.testkit;

import java.util.Collections;
import java.util.List;

public class ControllerNode implements TestKitNode {
    public static class Builder {
        private int id = -1;
        private String logDirectory = null;

        public Builder setId(int id) {
            this.id = id;
            return this;
        }

        public Builder setLogDirectory() {
            this.logDirectory = logDirectory;
            return this;
        }

        public ControllerNode build() {
            if (id == -1) {
                throw new RuntimeException("You must set the node id");
            }
            if (logDirectory == null) {
                logDirectory = String.format("controller_%d", id);
            }
            return new ControllerNode(id, logDirectory);
        }
    }

    private final int id;
    private final String logDirectory;

    private ControllerNode(int id, String logDirectory) {
        this.id = id;
        this.logDirectory = logDirectory;
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public List<String> logDirectories() {
        return Collections.singletonList(logDirectory);
    }
}
