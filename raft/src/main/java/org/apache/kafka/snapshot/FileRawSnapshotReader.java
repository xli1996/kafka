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
package org.apache.kafka.snapshot;

import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.UnalignedRecords;
import org.apache.kafka.raft.OffsetAndEpoch;

import java.io.IOException;
import java.nio.file.Path;

public final class FileRawSnapshotReader implements RawSnapshotReader {
    private final FileRecords fileRecords;
    private final OffsetAndEpoch snapshotId;

    private FileRawSnapshotReader(FileRecords fileRecords, OffsetAndEpoch snapshotId) {
        this.fileRecords = fileRecords;
        this.snapshotId = snapshotId;
    }

    @Override
    public OffsetAndEpoch snapshotId() {
        return snapshotId;
    }

    @Override
    public long sizeInBytes() {
        return fileRecords.sizeInBytes();
    }

    @Override
    public UnalignedRecords slice(long position, int size) {
        return fileRecords.sliceUnaligned(Math.toIntExact(position), size);
    }

    @Override
    public Records records() {
        return fileRecords;
    }

    @Override
    public void close() throws IOException {
        fileRecords.close();
    }

    /**
     * Opens a snapshot for reading.
     *
     * @param logDir the directory for the topic partition
     * @param snapshotId the end offset and epoch for the snapshotId
     * @throws java.nio.file.NoSuchFileException if the snapshot doesn't exist
     * @throws IOException for any IO error while opening the snapshot
     */
    public static FileRawSnapshotReader open(Path logDir, OffsetAndEpoch snapshotId) throws IOException {
        FileRecords fileRecords = FileRecords.open(
            Snapshots.snapshotPath(logDir, snapshotId).toFile(),
            false, // mutable
            true, // fileAlreadyExists
            0, // initFileSize
            false // preallocate
        );

        return new FileRawSnapshotReader(fileRecords, snapshotId);
    }
}
