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

package org.apache.streampark.console.base.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;

import javax.websocket.server.ServerContainer;

import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThat;

class WebSocketConfigTest {

    private static final String SERVER_CONTAINER_ATTRIBUTE = "javax.websocket.server.ServerContainer";

    private final WebApplicationContextRunner runner = new WebApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(WebSocketConfig.class))
        .withInitializer(
            context -> context.getServletContext()
                .setAttribute(SERVER_CONTAINER_ATTRIBUTE, fakeServerContainer()));

    @Test
    void exporterRegisteredByDefault() {
        runner.run(context -> assertThat(context).hasSingleBean(ServerEndpointExporter.class));
    }

    @Test
    void exporterRegisteredWhenExplicitlyEnabled() {
        runner.withPropertyValues("websocket.enabled=true")
            .run(context -> assertThat(context).hasSingleBean(ServerEndpointExporter.class));
    }

    @Test
    void exporterAbsentWhenDisabled() {
        runner.withPropertyValues("websocket.enabled=false")
            .run(context -> assertThat(context).doesNotHaveBean(ServerEndpointExporter.class));
    }

    /** ServerEndpointExporter needs a ServerContainer attribute to initialize. */
    private static ServerContainer fakeServerContainer() {
        return (ServerContainer) Proxy.newProxyInstance(
            WebSocketConfigTest.class.getClassLoader(),
            new Class<?>[]{ServerContainer.class},
            (proxy, method, args) -> null);
    }
}
