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

package org.apache.streampark.console.core.service.alert.impl;

import org.apache.streampark.console.core.bean.AlertTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Midea robot (美信) alert notify. Piggybacks on the email alert chain: apps with an email alert
 * configured also push a message to the robot. Failure is logged only and never breaks the email
 * alert chain.
 */
@Slf4j
@Service
public class MideaRobotAlertNotify {

    private static final String MSG =
        "【StreamPark-实时流】发生告警：\n作业名称：%s\n开始时间：%s\n结束时间：%s\n作业状态：%s\n";

    private final RestTemplate alertRestTemplate;

    private final ObjectMapper objectMapper;

    @Value("${midea.robot.url:}")
    private String robotUrl;

    public MideaRobotAlertNotify(RestTemplate alertRestTemplate, ObjectMapper objectMapper) {
        this.alertRestTemplate = alertRestTemplate;
        this.objectMapper = objectMapper;
    }

    public void alert(AlertTemplate template, String[] mails) {
        if (robotUrl == null || robotUrl.isEmpty()) {
            log.warn("MideaRobotAlertNotify skipped: midea.robot.url is not configured");
            return;
        }
        List<String> userCodes = new ArrayList<>();
        for (String mail : mails) {
            String[] split = mail.split("@");
            if (split.length > 0 && !split[0].isEmpty()) {
                userCodes.add(split[0]);
            }
        }
        if (userCodes.isEmpty()) {
            return;
        }
        try {
            String body = String.format(
                MSG,
                template.getJobName(),
                template.getStartTime(),
                template.getEndTime(),
                template.getStatus());

            Map<String, Object> queryMap = new HashMap<>(8);
            queryMap.put("subType", 1);
            queryMap.put("type", 1);
            queryMap.put("atPerson", 2);
            queryMap.put("body", body);
            queryMap.put("atAccounts", userCodes);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> entity = new HttpEntity<>(objectMapper.writeValueAsString(queryMap), headers);

            ResponseEntity<String> response = alertRestTemplate.postForEntity(robotUrl, entity, String.class);
            log.info("MideaRobotAlertNotify response: {}", response.getBody());
        } catch (Exception e) {
            log.error("MideaRobotAlertNotify failed", e);
        }
    }
}
