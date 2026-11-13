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

package org.apache.streampark.console.kubernetes.service.impl;

import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.kubernetes.client.K8sClientManager;
import org.apache.streampark.console.kubernetes.entity.K8sCluster;
import org.apache.streampark.console.kubernetes.mapper.K8sClusterMapper;
import org.apache.streampark.console.kubernetes.service.K8sClusterService;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class K8sClusterServiceImpl extends ServiceImpl<K8sClusterMapper, K8sCluster>
    implements
        K8sClusterService {

    private static final String NAME_EXISTED_HINT =
        "The kubernetes cluster name existed already. Try on a new name, please.";

    @Autowired
    private K8sClientManager k8sClientManager;

    @Override
    public boolean createCluster(K8sCluster cluster) {
        ApiAlertException.throwIfTrue(
            this.lambdaQuery().eq(K8sCluster::getClusterName, cluster.getClusterName()).exists(),
            NAME_EXISTED_HINT);
        return save(cluster);
    }

    @Override
    public void updateCluster(K8sCluster cluster) {
        K8sCluster fromDb = getById(cluster.getId());
        ApiAlertException.throwIfNull(fromDb, "The kubernetes cluster doesn't exist.");
        ApiAlertException.throwIfTrue(
            this.lambdaQuery()
                .eq(K8sCluster::getClusterName, cluster.getClusterName())
                .ne(K8sCluster::getId, cluster.getId())
                .exists(),
            NAME_EXISTED_HINT);
        updateById(cluster);
        if (fromDb.getKubeconfig() != null && !fromDb.getKubeconfig().equals(cluster.getKubeconfig())) {
            k8sClientManager.invalidate(cluster.getId());
        }
    }

    @Override
    public void remove(K8sCluster cluster) {
        ApiAlertException.throwIfNull(cluster.getId(), "Cluster id mustn't be null.");
        removeById(cluster.getId());
        k8sClientManager.invalidate(cluster.getId());
    }

    @Override
    public List<K8sCluster> listAll() {
        return this.lambdaQuery().orderByAsc(K8sCluster::getId).list();
    }
}
