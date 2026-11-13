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

package org.apache.streampark.console.core.service.application.impl;

import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.service.application.FlinkApplicationActionService;
import org.apache.streampark.console.core.service.application.FlinkApplicationBatchService;
import org.apache.streampark.console.core.service.application.FlinkApplicationBuildPipelineService;
import org.apache.streampark.console.core.service.application.FlinkApplicationInfoService;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class FlinkApplicationBatchServiceImpl implements FlinkApplicationBatchService {

    private static final String INVALID_PARAM = "请确定传参ids和teamId";

    @Autowired
    private FlinkApplicationActionService applicationActionService;

    @Autowired
    private FlinkApplicationInfoService applicationInfoService;

    @Autowired
    private FlinkApplicationBuildPipelineService appBuildPipeService;

    @Override
    public String batchStart(List<Long> ids, Long teamId) {
        StringBuilder messages = new StringBuilder();
        for (FlinkApplication app : loadTeamApps(ids, teamId, messages)) {
            try {
                applicationInfoService.checkEnv(app);
                applicationActionService.start(app, false);
            } catch (Exception e) {
                log.error("batch start app {} failed", app.getId(), e);
                collectError(messages, app.getJobName(), e);
            }
        }
        return messages.toString();
    }

    @Override
    public String batchCancel(List<Long> ids, Long teamId) {
        StringBuilder messages = new StringBuilder();
        for (FlinkApplication app : loadTeamApps(ids, teamId, messages)) {
            try {
                applicationActionService.cancel(app);
            } catch (Exception e) {
                log.error("batch cancel app {} failed", app.getId(), e);
                collectError(messages, app.getJobName(), e);
            }
        }
        return messages.toString();
    }

    @Override
    public String batchAbort(List<Long> ids, Long teamId) {
        StringBuilder messages = new StringBuilder();
        for (FlinkApplication app : loadTeamApps(ids, teamId, messages)) {
            try {
                applicationActionService.abort(app.getId());
            } catch (Exception e) {
                log.error("batch abort app {} failed", app.getId(), e);
                collectError(messages, app.getJobName(), e);
            }
        }
        return messages.toString();
    }

    @Override
    public String batchBuild(List<Long> ids, Long teamId, boolean forceBuild) {
        StringBuilder messages = new StringBuilder();
        for (FlinkApplication app : loadTeamApps(ids, teamId, messages)) {
            try {
                boolean submitted = appBuildPipeService.buildApplication(app.getId(), forceBuild);
                if (!submitted) {
                    messages.append("任务[").append(app.getJobName())
                        .append("]-异常信息: build application failed, please check the build log\n");
                }
            } catch (Exception e) {
                log.error("batch build app {} failed", app.getId(), e);
                collectError(messages, app.getJobName(), e);
            }
        }
        return messages.toString();
    }

    private List<FlinkApplication> loadTeamApps(List<Long> ids, Long teamId, StringBuilder messages) {
        if (ids == null || ids.isEmpty() || teamId == null) {
            messages.append(INVALID_PARAM).append('\n');
            return List.of();
        }
        return ids.stream()
            .map(id -> {
                FlinkApplication persist = applicationActionService.getById(id);
                if (persist == null || !teamId.equals(persist.getTeamId())) {
                    messages.append("任务[").append(persist == null ? id : persist.getJobName())
                        .append("]-异常信息: 不属于当前team\n");
                    return null;
                }
                return persist;
            })
            .filter(app -> app != null)
            .collect(Collectors.toList());
    }

    private void collectError(StringBuilder messages, String jobName, Exception e) {
        messages.append("任务[").append(jobName)
            .append("]-异常信息:").append(e.getMessage()).append('\n');
    }
}
