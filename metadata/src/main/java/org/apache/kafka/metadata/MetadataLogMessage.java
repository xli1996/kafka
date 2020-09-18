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

package org.apache.kafka.metadata;

/**
 * Representation of a message from the metadata log that can be
 * ingested onto an existing stable basis to create a new stable basis.
 *
 * Throwaway draft just to help flesh out the code.
 */
public interface MetadataLogMessage {
    MetadataLogMessage EMPTY_SNAPSHOT_MESSAGE = new MetadataLogMessage() {
        @Override
        public int leaderEpoch() {
            return Integer.MIN_VALUE;
        }

        @Override
        public long offset() {
            return Long.MIN_VALUE;
        }

        @Override
        public boolean isSnapshotMessage() {
            return true;
        }
    };

    /**
     * @return the leader epoch of the basis
     */
    int leaderEpoch();

    /**
     * @return the offset of the basis
     */
    long offset();

    /**
     * @return true if this message represents a snapshot, otherwise false
     */
    boolean isSnapshotMessage();
}
