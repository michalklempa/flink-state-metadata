/*
 * Copyright 2020 Michal Klempa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.michalklempa.flink.state.metadata;

import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import java.io.DataInputStream;
import java.io.FileInputStream;

public interface Load {
    default CheckpointMetadata load(String filename) throws Exception {
        CheckpointMetadata result = null;
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(filename))) {
            String externalPointer = null;
            result = Checkpoints.loadCheckpointMetadata(dataInputStream, classLoader, externalPointer);
        }
        return result;
    }

    class Default implements Load {
    }
}
