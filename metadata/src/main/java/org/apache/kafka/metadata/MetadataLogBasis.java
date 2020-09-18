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

import java.util.Objects;

/**
 * Representation of a stable basis for the ingested metadata log
 *
 * Throwaway draft just to help flesh out the code.
 */
public interface MetadataLogBasis {
    /**
     * @return the leader epoch of the basis
     */
    int leaderEpoch();

    /**
     * @return the offset of the basis
     */
    long offset();

    /**
     * @return true if this basis was generated from a snapshot message as opposed to a single non-snapshot message
     */
    boolean isFromSnapshot();

    /**
     * Ingest a message onto this stable basis to generate a new stable basis
     *
     * @param message the message to ingest
     * @return the new stable basis created by applying the given message on top of this stable basis
     */
    MetadataLogBasis ingest(MetadataLogMessage message);

    /**
     *
     * @param message the mandatory message
     * @return true if this basis contains the given message, otherwise false
     */
    default boolean contains(MetadataLogMessage message) {
        Objects.requireNonNull(message);
        int messageLeaderEpoch = message.leaderEpoch();
        int basisLeaderEpoch = leaderEpoch();
        return messageLeaderEpoch == basisLeaderEpoch ? message.offset() <= offset() : messageLeaderEpoch < basisLeaderEpoch;
    }
}
