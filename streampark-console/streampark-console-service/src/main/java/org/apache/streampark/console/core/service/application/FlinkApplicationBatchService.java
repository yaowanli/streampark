/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.streampark.console.core.service.application;

import java.util.List;

/**
 * Batch operations for flink applications, shared by the console controller and the OpenAPI
 * controller.
 *
 * <p>Each method returns an empty string when every app succeeded, otherwise a multi-line message
 * with per-app errors.
 */
public interface FlinkApplicationBatchService {

    String batchStart(List<Long> ids, Long teamId);

    String batchCancel(List<Long> ids, Long teamId);

    String batchAbort(List<Long> ids, Long teamId);

    String batchBuild(List<Long> ids, Long teamId, boolean forceBuild);
}
