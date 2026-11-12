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

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class PodResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    private String name;

    private String namespace;

    private String nodeName;

    private String phase;

    private Long restarts;

    /** -1 when metrics-server unavailable. */
    private Long cpuMillis;

    /** -1 when metrics-server unavailable. */
    private Long memoryBytes;

    private String ip;

    /** ISO8601 creation timestamp. */
    private String creationTimestamp;
}
