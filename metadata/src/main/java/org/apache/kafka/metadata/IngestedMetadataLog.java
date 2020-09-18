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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Representation of an ingested metadata log.
 *
 * Throwaway draft just to help flesh out the code.
 */
public interface IngestedMetadataLog {
    Future<String> CLUSTER_ID_FUTURE = new CompletableFuture<>();

    /**
     * Calling get() on the returned future will block if the broker has just started
     * and the metadata log has not yet been ingested up to the high watermark.
     * Once ingestion up to the high watermark has been done at least once
     * then no returned future will block upon calling get().
     *
     * This is one possibility for forcing the broker to wait for the log to be fully ingested.
     *
     * @return A future for the most recent stable basis of the ingested metadata log
     */
    Future<MetadataLogBasis> latestBasis();

    /**
     * Calling get() on the returned future will block if the broker has just started
     * and the metadata log has not yet been ingested to the point where the clusterId is known.
     *
     * @return a future to the cluster ID according to the metadata log
     */
    default Future<String> clusterId() {
        return CLUSTER_ID_FUTURE;
    }
}
