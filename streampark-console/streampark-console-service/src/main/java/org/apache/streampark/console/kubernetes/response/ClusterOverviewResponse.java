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

package org.apache.streampark.console.kubernetes.response;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/** Cluster-level realtime overview. Resource usage is -1 when metrics-server is unavailable. */
@Getter
@Setter
public class ClusterOverviewResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonSerialize(using = LongAsNumberSerializer.class)
    private Long nodesTotal;

    @JsonSerialize(using = LongAsNumberSerializer.class)
    private Long nodesReady;

    @JsonSerialize(using = LongAsNumberSerializer.class)
    private Long podsRunning;

    @JsonSerialize(using = LongAsNumberSerializer.class)
    private Long podsPending;

    @JsonSerialize(using = LongAsNumberSerializer.class)
    private Long podsFailed;

    @JsonSerialize(using = LongAsNumberSerializer.class)
    private Long namespaces;

    /** cpu allocatable, millis. */
    @JsonSerialize(using = LongAsNumberSerializer.class)
    private Long cpuAllocatableMillis;

    /** cpu usage, millis; -1 when metrics-server unavailable. */
    @JsonSerialize(using = LongAsNumberSerializer.class)
    private Long cpuUsedMillis;

    /** memory allocatable, bytes. */
    @JsonSerialize(using = LongAsNumberSerializer.class)
    private Long memAllocatableBytes;

    /** memory usage, bytes; -1 when metrics-server unavailable. */
    @JsonSerialize(using = LongAsNumberSerializer.class)
    private Long memUsedBytes;

    private Boolean metricsServerAvailable;
}
