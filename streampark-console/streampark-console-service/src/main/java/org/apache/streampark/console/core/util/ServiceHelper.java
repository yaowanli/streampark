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

package org.apache.streampark.console.core.util;

import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.util.SpringContextUtils;
import org.apache.streampark.console.base.util.WebUtils;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.entity.FlinkEnv;
import org.apache.streampark.console.core.entity.SparkEnv;
import org.apache.streampark.console.system.authentication.JWTUtil;
import org.apache.streampark.console.system.entity.User;
import org.apache.streampark.console.system.service.UserService;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.AuthenticationException;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class ServiceHelper {

    private static String flinkSqlClientJar = null;

    private static String sparkSqlClientJar = null;

    /** The latest universal CDC release train, also the fallback for unknown versions. */
    private static final String DEFAULT_CDC_CLIENT_SUFFIX = "cdc35";

    /** Minors supported by the bundled CDC client jars: 3.2, 3.3, 3.4 and 3.5. */
    private static final int MIN_CDC_MINOR = 2;

    private static final int MAX_CDC_MINOR = 5;

    private static final Map<String, String> cdcClientJars = new ConcurrentHashMap<>();

    public static User getLoginUser() {
        String token = (String) SecurityUtils.getSubject().getPrincipal();
        Long userId = JWTUtil.getUserId(token);
        if (userId == null) {
            throw new AuthenticationException("Unauthorized");
        }
        return SpringContextUtils.getBean(UserService.class).getById(userId);
    }

    public static Long getUserId() {
        User user = getLoginUser();
        if (user != null) {
            return user.getUserId();
        }
        return null;
    }

    public static String getFlinkSqlClientJar(FlinkEnv flinkEnv) {
        if (flinkSqlClientJar == null) {
            flinkSqlClientJar = findClientJar(
                "streampark-flink-sqlclient-.*\\.jar", "streampark-flink-sqlclient");
        }
        return flinkSqlClientJar;
    }

    /**
     * Selects the CDC client jar matching the pipeline connector version declared by the
     * application dependencies, falling back to the default jar with a warning when the detected
     * version is not bundled.
     */
    public static String getFlinkCDCClientJar(FlinkEnv flinkEnv, FlinkApplication app) {
        Integer minor = FlinkCdcUtils.detectConnectorMinorVersion(app.getDependencyObject())
            .orElse(null);
        String suffix = resolveCdcClientSuffix(minor);
        return cdcClientJars.computeIfAbsent(suffix, ServiceHelper::findCdcClientJar);
    }

    private static String resolveCdcClientSuffix(Integer minor) {
        if (minor == null) {
            return DEFAULT_CDC_CLIENT_SUFFIX;
        }
        if (minor < MIN_CDC_MINOR || minor > MAX_CDC_MINOR) {
            log.warn(
                "Detected flink cdc connector version 3.{} is outside the bundled range [3.{}, 3.{}],"
                    + " fallback to the {} client jar for soft compatibility.",
                minor, MIN_CDC_MINOR, MAX_CDC_MINOR, DEFAULT_CDC_CLIENT_SUFFIX);
            return DEFAULT_CDC_CLIENT_SUFFIX;
        }
        return "cdc3" + minor;
    }

    private static String findCdcClientJar(String suffix) {
        String regex = "streampark-flink-cdcclient-" + suffix + "-.*\\.jar";
        File localClient = WebUtils.getAppClientDir();
        List<String> jars = listClientJars(regex, localClient);
        if (jars.isEmpty() && !DEFAULT_CDC_CLIENT_SUFFIX.equals(suffix)) {
            log.warn(
                "No {} cdc client jar found in {}, fallback to the {} client jar.",
                suffix, localClient, DEFAULT_CDC_CLIENT_SUFFIX);
            suffix = DEFAULT_CDC_CLIENT_SUFFIX;
            regex = "streampark-flink-cdcclient-" + suffix + "-.*\\.jar";
            jars = listClientJars(regex, localClient);
        }
        ApiAlertException.throwIfTrue(
            jars.isEmpty(), "[StreamPark] can't found streampark-flink-" + suffix + " jar in " + localClient);
        return jars.get(0);
    }

    private static List<String> listClientJars(String regex, File localClient) {
        ApiAlertException.throwIfFalse(
            localClient.exists(), "[StreamPark] " + localClient + " no exists. please check.");
        return Arrays.stream(Objects.requireNonNull(localClient.list()))
            .filter(x -> x.matches(regex))
            .collect(Collectors.toList());
    }

    private static String findClientJar(String regex, String jarName) {
        File localClient = WebUtils.getAppClientDir();
        ApiAlertException.throwIfFalse(
            localClient.exists(), "[StreamPark] " + localClient + " no exists. please check.");

        List<String> jars = Arrays.stream(Objects.requireNonNull(localClient.list()))
            .filter(x -> x.matches(regex))
            .collect(Collectors.toList());

        ApiAlertException.throwIfTrue(
            jars.isEmpty(), "[StreamPark] can't found " + jarName + " jar in " + localClient);

        ApiAlertException.throwIfTrue(
            jars.size() > 1, "[StreamPark] found multiple " + jarName + " jar in " + localClient);
        return jars.get(0);
    }

    public static String getSparkSqlClientJar(SparkEnv sparkEnv) {
        if (sparkSqlClientJar == null) {
            File localClient = WebUtils.getAppClientDir();
            ApiAlertException.throwIfFalse(
                localClient.exists(), "[StreamPark] " + localClient + " no exists. please check.");
            List<String> jars = Arrays.stream(Objects.requireNonNull(localClient.list()))
                .filter(
                    x -> x.matches(
                        "streampark-spark-sqlclient_" + sparkEnv.getScalaVersion()
                            + "-.*\\.jar"))
                .collect(Collectors.toList());

            ApiAlertException.throwIfTrue(
                jars.isEmpty(),
                "[StreamPark] can't found streampark-flink-sqlclient jar in " + localClient);

            ApiAlertException.throwIfTrue(
                jars.size() > 1,
                "[StreamPark] found multiple streampark-flink-sqlclient jar in " + localClient);

            sparkSqlClientJar = jars.get(0);
        }
        return sparkSqlClientJar;
    }
}
