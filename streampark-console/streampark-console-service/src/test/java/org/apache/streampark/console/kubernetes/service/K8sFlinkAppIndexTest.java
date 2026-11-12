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

package org.apache.streampark.console.kubernetes.service;

import org.apache.streampark.common.enums.FlinkDeployMode;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.kubernetes.service.impl.K8sMonitorServiceImpl;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class K8sFlinkAppIndexTest {

    private FlinkApplication app(long id, String ns, String jobName, int deployMode) {
        FlinkApplication app = new FlinkApplication();
        app.setId(id);
        app.setK8sNamespace(ns);
        app.setJobName(jobName);
        app.setDeployMode(deployMode);
        return app;
    }

    @Test
    void testStreamParkAppIdOfMatchesNamespaceAndJobName() {
        // streamParkAppIdOf is mode-agnostic: mode filtering happens in loadK8sAppsById()
        Map<Long, FlinkApplication> apps = new HashMap<>();
        apps.put(1L, app(1L, "default", "app-one",
            FlinkDeployMode.KUBERNETES_NATIVE_APPLICATION.getMode()));

        assertThat(K8sMonitorServiceImpl.streamParkAppIdOf("default", "app-one", apps)).isEqualTo(1L);
        assertThat(K8sMonitorServiceImpl.streamParkAppIdOf("other-ns", "app-one", apps)).isNull();
        assertThat(K8sMonitorServiceImpl.streamParkAppIdOf("default", "app-one-sub", apps)).isNull();
        assertThat(K8sMonitorServiceImpl.streamParkAppIdOf("default", "other", apps)).isNull();
    }
}
