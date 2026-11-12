# StreamPark K8S Monitoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 StreamPark 上新增多 K8S 集群注册管理与运行监控（节点/工作负载/事件实时值 + 可选 Prometheus 历史趋势 + StreamPark Flink 应用标注）。

**Architecture:** console-service 新增 `org.apache.streampark.console.kubernetes` 包（fabric8 kubernetes-client 直连 K8S API + metrics.k8s.io；Prometheus HTTP 可选）；前端 `views/kubernetes/` 新增集群管理与监控页面（echarts 图表）。菜单由 t_menu 驱动。

**Tech Stack:** fabric8 kubernetes-client 6.13.4、Spring MVC + MyBatis-Plus（现有）、Jackson、Vue3 + ant-design-vue 3 + echarts 5、pnpm。

**Spec:** `docs/superpowers/specs/2026-11-12-k8s-monitoring-design.md`

**关键现状参考（写代码时对照）:**
- CRUD 模板：`YarnQueueController/Service/ServiceImpl/Entity/Mapper/Assembler`（`core` 包）
- 前端模板：`views/setting/yarn-queue/`、`api/setting/yarnQueue.ts`
- 菜单：`assembly/script/data/{mysql,pgsql}-data.sql` t_menu，一级菜单 110000~150000 已占用，本功能用 **160000** 段
- 启动类 `StreamParkConsoleBootstrap` 在 `org.apache.streampark.console`，默认扫描全部子包，新包 `console.kubernetes` 自动生效
- watcher：`FlinkKubernetesWatcher.getAllJobStatus()` 返回 `Map<CacheKey, JobStatusCV>`，`CacheKey.key()` = appId；`JobStatusCV.jobState()` 返回 `FlinkJobState`

---

### Task 1: 数据库 schema（t_k8s_cluster）

**Files:**
- Modify: `streampark-console/streampark-console-service/src/main/assembly/script/schema/mysql-schema.sql`（文件末尾 `set foreign_key_checks = 1;` 之前）
- Modify: `streampark-console/streampark-console-service/src/main/assembly/script/schema/pgsql-schema.sql`（文件末尾）
- Modify: `streampark-console/streampark-console-service/src/main/resources/db/schema-h2.sql`（参照同文件 t_yarn_queue 的 H2 语法，~L505）

- [ ] **Step 1: mysql-schema.sql 末尾（`set foreign_key_checks = 1;` 之前）追加**

```sql

-- ----------------------------
-- table structure for t_k8s_cluster
-- ----------------------------
drop table if exists `t_k8s_cluster`;
create table `t_k8s_cluster` (
  `id` bigint not null primary key auto_increment comment 'cluster id',
  `cluster_name` varchar(128) collate utf8mb4_general_ci not null comment 'display name of the kubernetes cluster',
  `kubeconfig` text collate utf8mb4_general_ci not null comment 'kubeconfig file content',
  `prometheus_url` varchar(255) collate utf8mb4_general_ci default null comment 'prometheus server url, nullable to use realtime only',
  `description` varchar(255) collate utf8mb4_general_ci default null comment 'description of the kubernetes cluster',
  `create_time` datetime default null comment 'create time',
  `modify_time` datetime default null comment 'modify time',
  unique key `unq_cluster_name` (`cluster_name`) using btree
) engine = innodb default charset = utf8mb4 collate = utf8mb4_general_ci;
```

- [ ] **Step 2: pgsql-schema.sql 末尾追加（参照该文件中 t_yarn_queue 的既有 pgsql 写法风格）**

```sql
-- ----------------------------
-- table structure for t_k8s_cluster
-- ----------------------------
drop table if exists t_k8s_cluster;
create table t_k8s_cluster (
  id bigserial primary key,
  cluster_name varchar(128) not null,
  kubeconfig text not null,
  prometheus_url varchar(255) default null,
  description varchar(255) default null,
  create_time timestamp default null,
  modify_time timestamp default null,
  constraint unq_k8s_cluster_name unique (cluster_name)
);
```

注意：先读该文件中 t_yarn_queue 的实际写法（bigserial vs bigint+sequence 等），保持一致。

- [ ] **Step 3: schema-h2.sql 追加（先读 t_yarn_queue 的 H2 版本 ~L505，完全模仿其语法）**

```sql
-- table structure for t_k8s_cluster
-- ----------------------------
drop table if exists t_k8s_cluster;
create table if not exists t_k8s_cluster (
  id bigint not null auto_increment,
  cluster_name varchar(128) not null,
  kubeconfig text not null,
  prometheus_url varchar(255) default null,
  description varchar(255) default null,
  create_time datetime default null,
  modify_time datetime default null,
  primary key (id),
  constraint unq_k8s_cluster_name unique (cluster_name)
);
```

若邻居表 H2 语法不同（如 `bigserial`、`timestamp`），以邻居为准。

- [ ] **Step 4: Commit**

```bash
git add streampark-console/streampark-console-service/src/main/assembly/script/schema/ streampark-console/streampark-console-service/src/main/resources/db/schema-h2.sql
git commit -m "[K8s] Add t_k8s_cluster schema for kubernetes cluster registration"
```

---

### Task 2: fabric8 依赖 + QuantityUtils + K8sClientManager

**Files:**
- Modify: `pom.xml`（根，dependencyManagement 段）
- Modify: `streampark-console/streampark-console-service/pom.xml`
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/client/K8sClientManager.java`
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/util/K8sQuantityUtils.java`
- Test: `streampark-console/streampark-console-service/src/test/java/org/apache/streampark/console/kubernetes/util/K8sQuantityUtilsTest.java`

- [ ] **Step 1: 根 pom.xml dependencyManagement 中 caffeine 所在区域附近加入 fabric8 BOM**

```xml
<dependency>
    <groupId>io.fabric8</groupId>
    <artifactId>kubernetes-client-bom</artifactId>
    <version>6.13.4</version>
    <type>pom</type>
    <scope>import</scope>
</dependency>
```

- [ ] **Step 2: console-service/pom.xml dependencies 加入（与现有依赖并列）**

```xml
<dependency>
    <groupId>io.fabric8</groupId>
    <artifactId>kubernetes-client</artifactId>
</dependency>
<dependency>
    <groupId>io.fabric8</groupId>
    <artifactId>kubernetes-server-mock</artifactId>
    <scope>test</scope>
</dependency>
```

- [ ] **Step 3: 验证依赖下载 + 编译环境**

Run: `mvn -q -pl streampark-console/streampark-console-service dependency:resolve -DincludeGroupIds=io.fabric8`
Expected: BUILD SUCCESS（列出 fabric8 构件）

- [ ] **Step 4: 写失败测试 K8sQuantityUtilsTest**

```java
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

package org.apache.streampark.console.kubernetes.util;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.Quantity;

import static org.assertj.core.api.Assertions.assertThat;

class K8sQuantityUtilsTest {

    @Test
    void testCpuToMillis() {
        assertThat(K8sQuantityUtils.cpuToMillis(new Quantity("2"))).isEqualTo(2000L);
        assertThat(K8sQuantityUtils.cpuToMillis(new Quantity("1500m"))).isEqualTo(1500L);
        assertThat(K8sQuantityUtils.cpuToMillis(new Quantity("250m"))).isEqualTo(250L);
        assertThat(K8sQuantityUtils.cpuToMillis(null)).isEqualTo(-1L);
    }

    @Test
    void testMemoryToBytes() {
        assertThat(K8sQuantityUtils.memoryToBytes(new Quantity("1Gi"))).isEqualTo(1073741824L);
        assertThat(K8sQuantityUtils.memoryToBytes(new Quantity("2048Ki"))).isEqualTo(2097152L);
        assertThat(K8sQuantityUtils.memoryToBytes(null)).isEqualTo(-1L);
    }
}
```

- [ ] **Step 5: 运行确认失败**

Run: `mvn -q -pl streampark-console/streampark-console-service test -Dtest=K8sQuantityUtilsTest`
Expected: 编译错误（K8sQuantityUtils 不存在）

- [ ] **Step 6: 实现 K8sQuantityUtils**

```java
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

package org.apache.streampark.console.kubernetes.util;

import io.fabric8.kubernetes.api.model.Quantity;

import javax.annotation.Nullable;

/** Kubernetes resource quantity parsing (cpu "1500m"/"2", memory "1Gi"/"2048Ki"). */
public final class K8sQuantityUtils {

    private K8sQuantityUtils() {
    }

    @Nullable
    public static long cpuToMillis(@Nullable Quantity quantity) {
        if (quantity == null || quantity.getAmount() == null) {
            return -1L;
        }
        String amount = quantity.getAmount().trim();
        try {
            if (amount.endsWith("m")) {
                return (long) Double.parseDouble(amount.substring(0, amount.length() - 1));
            }
            return (long) (Double.parseDouble(amount) * 1000);
        } catch (NumberFormatException ignored) {
            return -1L;
        }
    }

    public static long memoryToBytes(@Nullable Quantity quantity) {
        if (quantity == null || quantity.getAmount() == null) {
            return -1L;
        }
        try {
            return Quantity.getAmountInBytes(quantity).longValue();
        } catch (Exception ignored) {
            return -1L;
        }
    }
}
```

- [ ] **Step 7: 运行测试确认通过**

Run: `mvn -q -pl streampark-console/streampark-console-service test -Dtest=K8sQuantityUtilsTest`
Expected: PASS（memoryToBytes 用 fabric8 `Quantity.getAmountInBytes`，支持 Ki/Mi/Gi；若 2048Ki 解析不符，以实际 fabric8 行为修正断言）

- [ ] **Step 8: 实现 K8sClientManager**

```java
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

package org.apache.streampark.console.kubernetes.client;

import org.springframework.stereotype.Component;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

import java.util.concurrent.ConcurrentHashMap;

/** Caches fabric8 KubernetesClient instances per registered cluster id. */
@Component
public class K8sClientManager {

    private static final int CONNECT_TIMEOUT = 10_000;
    private static final int REQUEST_TIMEOUT = 20_000;

    private final ConcurrentHashMap<Long, KubernetesClient> clients = new ConcurrentHashMap<>();

    public KubernetesClient getOrCreate(Long clusterId, String kubeconfig) {
        KubernetesClient client = clients.get(clusterId);
        if (client != null) {
            return client;
        }
        synchronized (this) {
            client = clients.get(clusterId);
            if (client == null) {
                client = buildClient(kubeconfig);
                clients.put(clusterId, client);
            }
            return client;
        }
    }

    public void invalidate(Long clusterId) {
        KubernetesClient client = clients.remove(clusterId);
        if (client != null) {
            client.close();
        }
    }

    /** Builds a throwaway client, used by connection test (no caching). */
    public KubernetesClient buildClient(String kubeconfig) {
        Config config = Config.fromKubeconfig(kubeconfig);
        config.setConnectionTimeout(CONNECT_TIMEOUT);
        config.setRequestTimeout(REQUEST_TIMEOUT);
        return new KubernetesClientBuilder().withConfig(config).build();
    }
}
```

注意：`Config.fromKubeconfig(String)` 在 fabric8 6.x 静态方法名为 `fromKubeconfig`（以实际编译为准）。

- [ ] **Step 9: Commit**

```bash
git add pom.xml streampark-console/streampark-console-service/pom.xml streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/ streampark-console/streampark-console-service/src/test/java/org/apache/streampark/console/kubernetes/
git commit -m "[K8s] Add fabric8 client dependency, K8sClientManager and quantity utils"
```

---

### Task 3: K8sCluster 实体/Mapper/DTO/Assembler/Service/Controller（CRUD + 测试连接）

**Files:**
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/entity/K8sCluster.java`
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/mapper/K8sClusterMapper.java`
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/request/K8sClusterCreateRequest.java`
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/request/K8sClusterUpdateRequest.java`
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/request/K8sClusterTestRequest.java`
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/response/K8sClusterResponse.java`
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/response/K8sClusterTestResponse.java`
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/assembler/K8sClusterAssembler.java`
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/service/K8sClusterService.java`
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/service/impl/K8sClusterServiceImpl.java`
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/controller/K8sClusterController.java`
- Test: `streampark-console/streampark-console-service/src/test/java/org/apache/streampark/console/kubernetes/service/K8sClusterServiceTest.java`

说明：所有文件均需 Apache License 头（拷贝 YarnQueue 同类文件的头注释）。`@Mapper` 注解：检查 `YarnQueueMapper` 是否有 `@Mapper` 注解或依赖 `@MapperScan`——保持与现有 mapper 一致的写法（已确认 YarnQueueMapper 无 @Mapper、纯 BaseMapper，说明有 MapperScan，按此办理）。

- [ ] **Step 1: 写失败测试 K8sClusterServiceTest（仿 YarnQueueServiceTest 结构，extends SpringUnitTestBase）**

```java
package org.apache.streampark.console.kubernetes.service;

import org.apache.streampark.console.SpringUnitTestBase;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.kubernetes.entity.K8sCluster;
import org.apache.streampark.console.kubernetes.service.impl.K8sClusterServiceImpl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
class K8sClusterServiceTest extends SpringUnitTestBase {

    @Autowired
    private K8sClusterService k8sClusterService;

    @AfterEach
    void clean() {
        k8sClusterService.remove(new K8sCluster());
    }

    private K8sCluster mockCluster(String name) {
        K8sCluster cluster = new K8sCluster();
        cluster.setClusterName(name);
        cluster.setKubeconfig("apiVersion: v1\nkind: Config");
        cluster.setPrometheusUrl(null);
        cluster.setDescription("test");
        return cluster;
    }

    @Test
    void testCreateAndDuplicateNameRejected() {
        assertThat(k8sClusterService.createCluster(mockCluster("test-cluster"))).isTrue();
        assertThatThrownBy(() -> k8sClusterService.createCluster(mockCluster("test-cluster")))
            .isInstanceOf(ApiAlertException.class);
    }

    @Test
    void testUpdateAndInvalidate() {
        K8sCluster cluster = mockCluster("c1");
        k8sClusterService.createCluster(cluster);
        K8sCluster fromDb = k8sClusterService.getById(cluster.getId());
        fromDb.setPrometheusUrl("http://prometheus:9090");
        k8sClusterService.updateCluster(fromDb);
        assertThat(k8sClusterService.getById(cluster.getId()).getPrometheusUrl())
            .isEqualTo("http://prometheus:9090");
    }

    @Test
    void testRemove() {
        K8sCluster cluster = mockCluster("c2");
        k8sClusterService.createCluster(cluster);
        k8sClusterService.remove(cluster);
        assertThat(k8sClusterService.getById(cluster.getId())).isNull();
    }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `mvn -q -pl streampark-console/streampark-console-service test -Dtest=K8sClusterServiceTest`
Expected: 编译错误（类不存在）

- [ ] **Step 3: 实现 Entity**

```java
package org.apache.streampark.console.kubernetes.entity;

import org.apache.streampark.console.base.mybatis.entity.BaseEntity;

import com.baomidou.mybatisplus.annotation.FieldStrategy;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
@TableName("t_k8s_cluster")
public class K8sCluster extends BaseEntity {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String clusterName;

    private String kubeconfig;

    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private String prometheusUrl;

    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private String description;
}
```

注意 `remove(new K8sCluster())`：MyBatis-Plus `remove(Wrapper)` 需要非 null wrapper；`Service.remove(entity)` 不存在——测试里改用 `k8sClusterService.remove(new QueryWrapper<>())`（仿 YarnQueueServiceTest 的 `flinkClusterService.remove(new QueryWrapper<>())`）。以 `com.baomidou.mybatisplus.core.conditions.query.QueryWrapper` 实现。

- [ ] **Step 4: 实现 Mapper**

```java
package org.apache.streampark.console.kubernetes.mapper;

import org.apache.streampark.console.kubernetes.entity.K8sCluster;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;

/** Kubernetes cluster mapper definition. */
public interface K8sClusterMapper extends BaseMapper<K8sCluster> {
}
```

- [ ] **Step 5: 实现 Request DTO**

K8sClusterCreateRequest：

```java
package org.apache.streampark.console.kubernetes.request;

import javax.validation.constraints.NotBlank;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class K8sClusterCreateRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    @NotBlank
    private String clusterName;

    @NotBlank
    private String kubeconfig;

    private String prometheusUrl;

    private String description;
}
```

K8sClusterUpdateRequest：

```java
package org.apache.streampark.console.kubernetes.request;

import javax.validation.constraints.NotNull;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class K8sClusterUpdateRequest extends K8sClusterCreateRequest {

    private static final long serialVersionUID = 1L;

    @NotNull
    private Long id;
}
```

K8sClusterTestRequest：

```java
package org.apache.streampark.console.kubernetes.request;

import javax.validation.constraints.NotBlank;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class K8sClusterTestRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    @NotBlank
    private String kubeconfig;
}
```

- [ ] **Step 6: 实现 Response DTO**

K8sClusterResponse：

```java
package org.apache.streampark.console.kubernetes.response;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;

/** API response for a kubernetes cluster registration. */
@Getter
@Setter
public class K8sClusterResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    private Long id;

    private String clusterName;

    private String prometheusUrl;

    private String description;

    private Date createTime;

    private Date modifyTime;
}
```

（注意：response 不含 kubeconfig 全文，避免列表接口泄露凭证。）

K8sClusterTestResponse：

```java
package org.apache.streampark.console.kubernetes.response;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class K8sClusterTestResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    private Boolean connected;

    private String version;

    private String message;
}
```

- [ ] **Step 7: 实现 Assembler（用 core 包 DtoAssembler 的 toDto）**

```java
package org.apache.streampark.console.kubernetes.assembler;

import org.apache.streampark.console.core.assembler.DtoAssembler;
import org.apache.streampark.console.kubernetes.entity.K8sCluster;
import org.apache.streampark.console.kubernetes.request.K8sClusterCreateRequest;
import org.apache.streampark.console.kubernetes.request.K8sClusterUpdateRequest;
import org.apache.streampark.console.kubernetes.response.K8sClusterResponse;

import org.springframework.beans.BeanUtils;

import java.util.List;
import java.util.stream.Collectors;

/** Converts between kubernetes cluster entities and API request/response contracts. */
public final class K8sClusterAssembler {

    private K8sClusterAssembler() {
    }

    public static K8sCluster toEntity(K8sClusterCreateRequest request) {
        if (request == null) {
            return null;
        }
        K8sCluster cluster = new K8sCluster();
        BeanUtils.copyProperties(request, cluster);
        return cluster;
    }

    public static K8sCluster toEntity(K8sClusterUpdateRequest request) {
        if (request == null) {
            return null;
        }
        K8sCluster cluster = toEntity((K8sClusterCreateRequest) request);
        cluster.setId(request.getId());
        return cluster;
    }

    public static K8sClusterResponse toResponse(K8sCluster cluster) {
        return DtoAssembler.toDto(cluster, K8sClusterResponse.class);
    }

    public static List<K8sClusterResponse> toListResponse(List<K8sCluster> clusters) {
        return clusters.stream().map(K8sClusterAssembler::toResponse).collect(Collectors.toList());
    }
}
```

若 `DtoAssembler.toDto` 签名不同，运行时以实际为准（先 grep `class DtoAssembler`）。

- [ ] **Step 8: 实现 Service 接口 + Impl**

接口：

```java
package org.apache.streampark.console.kubernetes.service;

import org.apache.streampark.console.kubernetes.entity.K8sCluster;

import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

public interface K8sClusterService extends IService<K8sCluster> {

    boolean createCluster(K8sCluster cluster);

    void updateCluster(K8sCluster cluster);

    void remove(K8sCluster cluster);

    List<K8sCluster> listAll();
}
```

Impl：

```java
package org.apache.streampark.console.kubernetes.service.impl;

import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.kubernetes.client.K8sClientManager;
import org.apache.streampark.console.kubernetes.entity.K8sCluster;
import org.apache.streampark.console.kubernetes.mapper.K8sClusterMapper;
import org.apache.streampark.console.kubernetes.service.K8sClusterService;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
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
```

（需要 `import lombok.extern.slf4j.Slf4j;`）

- [ ] **Step 9: 实现 Controller**

```java
package org.apache.streampark.console.kubernetes.controller;

import org.apache.streampark.console.base.domain.RestResponseBody;
import org.apache.streampark.console.base.web.FormOrJson;
import org.apache.streampark.console.kubernetes.assembler.K8sClusterAssembler;
import org.apache.streampark.console.kubernetes.client.K8sClientManager;
import org.apache.streampark.console.kubernetes.entity.K8sCluster;
import org.apache.streampark.console.kubernetes.request.K8sClusterCreateRequest;
import org.apache.streampark.console.kubernetes.request.K8sClusterTestRequest;
import org.apache.streampark.console.kubernetes.request.K8sClusterUpdateRequest;
import org.apache.streampark.console.kubernetes.response.K8sClusterResponse;
import org.apache.streampark.console.kubernetes.response.K8sClusterTestResponse;
import org.apache.streampark.console.kubernetes.service.K8sClusterService;

import org.apache.shiro.authz.annotation.RequiresPermissions;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

@Slf4j
@Validated
@RestController
@RequestMapping("k8s/cluster")
public class K8sClusterController {

    @Autowired
    private K8sClusterService k8sClusterService;

    @Autowired
    private K8sClientManager k8sClientManager;

    @PostMapping("list")
    public RestResponseBody<java.util.List<K8sClusterResponse>> list() {
        return RestResponseBody.success(
            K8sClusterAssembler.toListResponse(k8sClusterService.listAll()));
    }

    @PostMapping("create")
    @RequiresPermissions("k8sCluster:create")
    public RestResponseBody<Boolean> create(@Valid @FormOrJson K8sClusterCreateRequest request) {
        return RestResponseBody.success(
            k8sClusterService.createCluster(K8sClusterAssembler.toEntity(request)));
    }

    @PostMapping("update")
    @RequiresPermissions("k8sCluster:update")
    public RestResponseBody<Void> update(@Valid @FormOrJson K8sClusterUpdateRequest request) {
        k8sClusterService.updateCluster(K8sClusterAssembler.toEntity(request));
        return RestResponseBody.success();
    }

    @PostMapping("delete")
    @RequiresPermissions("k8sCluster:delete")
    public RestResponseBody<Void> delete(@Valid @FormOrJson K8sClusterUpdateRequest request) {
        k8sClusterService.remove(K8sClusterAssembler.toEntity(request));
        return RestResponseBody.success();
    }

    @PostMapping("test")
    public RestResponseBody<K8sClusterTestResponse> test(
        @Valid @FormOrJson K8sClusterTestRequest request) {
        K8sClusterTestResponse response = new K8sClusterTestResponse();
        try (KubernetesClient client = k8sClientManager.buildClient(request.getKubeconfig())) {
            response.setConnected(true);
            response.setVersion(client.getKubernetesVersion().getGitVersion());
            response.setMessage("Connection succeeded.");
        } catch (KubernetesClientException | IllegalArgumentException e) {
            log.warn("Kubernetes cluster connection test failed: {}", e.getMessage());
            response.setConnected(false);
            response.setMessage(e.getMessage());
        }
        return RestResponseBody.success(response);
    }
}
```

说明：list 用 POST 与 yarnQueue 一致（前端 defHttp.post）。`client.getKubernetesVersion()` 返回 `VersionInfo`，gitVersion 形如 "v1.29.4"；若编译期发现签名不同，改为 `client.getVersion().getGitVersion()`。

- [ ] **Step 10: 运行测试确认通过**

Run: `mvn -q -pl streampark-console/streampark-console-service test -Dtest=K8sClusterServiceTest`
Expected: PASS。若 H2 建表失败，检查 Task 1 Step 3 的 H2 DDL。

- [ ] **Step 11: 编译整个 service 模块**

Run: `mvn -q -pl streampark-console/streampark-console-service -am compile -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 12: Commit**

```bash
git add streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/ streampark-console/streampark-console-service/src/test/java/org/apache/streampark/console/kubernetes/
git commit -m "[K8s] Add kubernetes cluster registration management (CRUD + connection test)"
```

---

### Task 4: K8sMonitorService（overview/nodes/workloads/events/namespaces）+ Controller

**Files:**
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/response/ClusterOverviewResponse.java`
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/response/NodeResponse.java`
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/response/DeploymentResponse.java`
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/response/PodResponse.java`
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/response/EventResponse.java`
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/service/K8sMonitorService.java`
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/service/impl/K8sMonitorServiceImpl.java`
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/controller/K8sMonitorController.java`
- Test: `streampark-console/streampark-console-service/src/test/java/org/apache/streampark/console/kubernetes/service/K8sMonitorServiceTest.java`

- [ ] **Step 1: 实现 Response VO（全部字段 public getter/setter，用 lombok，Serializable，模式同 K8sClusterResponse；此处列出字段定义）**

ClusterOverviewResponse：

```java
private Long nodesTotal;          private Long nodesReady;
private Long podsRunning;         private Long podsPending;         private Long podsFailed;
private Long namespaces;
private Long cpuAllocatableMillis;  private Long cpuUsedMillis;
private Long memAllocatableBytes;   private Long memUsedBytes;
private Long warningEvents;
private Boolean metricsServerAvailable;
```

NodeResponse：

```java
private String name;  private String roles;  private String version;  private String status;
private String internalIp;  private Long podsCount;
private Long cpuAllocatableMillis;  private Long cpuUsedMillis;
private Long memAllocatableBytes;   private Long memUsedBytes;
```

DeploymentResponse：

```java
private String name;  private String namespace;
private Integer replicasReady;  private Integer replicasDesired;
private String image;  private Date creationTimestamp;
private Long streamParkAppId;    private String streamParkAppName;
```

PodResponse：

```java
private String name;  private String namespace;  private String nodeName;  private String phase;
private Long restarts;  private Long cpuMillis;  private Long memoryBytes;
private String ip;  private Date creationTimestamp;
```

EventResponse：

```java
private String type;  private String reason;  private String object;  private String message;
private Integer count;  private String lastTimestamp;
```

- [ ] **Step 2: 实现核心 Service（monitor 逻辑集中在可测的以 KubernetesClient 为参的方法上）**

接口：

```java
package org.apache.streampark.console.kubernetes.service;

import org.apache.streampark.console.kubernetes.entity.K8sCluster;
import org.apache.streampark.console.kubernetes.response.ClusterOverviewResponse;
import org.apache.streampark.console.kubernetes.response.DeploymentResponse;
import org.apache.streampark.console.kubernetes.response.EventResponse;
import org.apache.streampark.console.kubernetes.response.NodeResponse;
import org.apache.streampark.console.kubernetes.response.PodResponse;

import io.fabric8.kubernetes.client.KubernetesClient;

import java.util.List;

public interface K8sMonitorService {

    ClusterOverviewResponse overview(K8sCluster cluster);

    List<NodeResponse> nodes(K8sCluster cluster);

    List<DeploymentResponse> deployments(K8sCluster cluster, String namespace, String keyword);

    List<PodResponse> pods(K8sCluster cluster, String namespace, String keyword);

    List<EventResponse> events(K8sCluster cluster, String namespace, int limit);

    List<String> namespaces(K8sCluster cluster);
}
```

Impl（关键点：metrics API 用 `client.top().nodes()` / `client.top().pods().inAnyNamespace()`；若编译签名有出入，以 fabric8 6.13 实际 API 微调；所有异常不吞——由 controller 统一异常处理转 ApiAlertException 消息）：

```java
package org.apache.streampark.console.kubernetes.service.impl;

import org.apache.streampark.console.kubernetes.client.K8sClientManager;
import org.apache.streampark.console.kubernetes.entity.K8sCluster;
import org.apache.streampark.console.kubernetes.response.ClusterOverviewResponse;
import org.apache.streampark.console.kubernetes.response.DeploymentResponse;
import org.apache.streampark.console.kubernetes.response.EventResponse;
import org.apache.streampark.console.kubernetes.response.NodeResponse;
import org.apache.streampark.console.kubernetes.response.PodResponse;
import org.apache.streampark.console.kubernetes.service.K8sMonitorService;
import org.apache.streampark.console.kubernetes.util.K8sQuantityUtils;
import org.apache.streampark.console.core.service.application.FlinkApplicationManageService;
import org.apache.streampark.console.core.entity.FlinkApplication;

import com.google.common.collect.Maps;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeAddress;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.metrics.v1beta1.NodeMetrics;
import io.fabric8.kubernetes.api.model.metrics.v1beta1.PodMetrics;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
public class K8sMonitorServiceImpl implements K8sMonitorService {

    @Autowired
    private K8sClientManager k8sClientManager;

    @Autowired
    private FlinkApplicationManageService applicationManageService;

    // ---------- overview ----------
    @Override
    public ClusterOverviewResponse overview(K8sCluster cluster) {
        try (KubernetesClient client = k8sClientManager.getOrCreate(cluster.getId(), cluster.getKubeconfig())) {
            List<Node> nodes = client.nodes().list().getItems();
            List<Pod> pods = client.pods().inAnyNamespace().list().getItems();
            ClusterOverviewResponse resp = new ClusterOverviewResponse();
            resp.setNodesTotal((long) nodes.size());
            resp.setNodesReady(nodes.stream().filter(this::isNodeReady).count());
            resp.setPodsRunning(pods.stream().filter(p -> "Running".equals(p.getStatus().getPhase())).count());
            resp.setPodsPending(pods.stream().filter(p -> "Pending".equals(p.getStatus().getPhase())).count());
            resp.setPodsFailed(pods.stream().filter(p -> "Failed".equals(p.getStatus().getPhase())).count());
            resp.setNamespaces((long) client.namespaces().list().getItems().size());

            long cpuAlloc = nodes.stream()
                .mapToLong(n -> K8sQuantityUtils.cpuToMillis(n.getStatus().getAllocatable().get("cpu")))
                .sum();
            long memAlloc = nodes.stream()
                .mapToLong(n -> K8sQuantityUtils.memoryToBytes(n.getStatus().getAllocatable().get("memory")))
                .sum();

            // metrics api (may be unavailable if metrics-server missing)
            Long cpuUsed = null;
            Long memUsed = null;
            try {
                List<NodeMetrics> nodeMetrics = client.top().nodes().getItems();
                cpuUsed = nodeMetrics.stream()
                    .mapToLong(m -> K8sQuantityUtils.cpuToMillis(m.getUsage().get("cpu"))).sum();
                memUsed = nodeMetrics.stream()
                    .mapToLong(m -> K8sQuantityUtils.memoryToBytes(m.getUsage().get("memory"))).sum();
                resp.setMetricsServerAvailable(true);
            } catch (Exception e) {
                resp.setMetricsServerAvailable(false);
                cpuUsed = -1L;
                memUsed = -1L;
            }
            resp.setCpuAllocatableMillis(cpuAlloc);
            resp.setCpuUsedMillis(cpuUsed);
            resp.setMemAllocatableBytes(memAlloc);
            resp.setMemUsedBytes(memUsed);
            return resp;
        }
    }

    // ---------- nodes ----------
    @Override
    public List<NodeResponse> nodes(K8sCluster cluster) {
        try (KubernetesClient client = k8sClientManager.getOrCreate(cluster.getId(), cluster.getKubeconfig())) {
            List<Node> nodes = client.nodes().list().getItems();
            Map<String, NodeMetrics> metricsByName;
            try {
                metricsByName = client.top().nodes().getItems().stream()
                    .collect(Collectors.toMap(NodeMetrics::getName, m -> m, (a, b) -> a));
            } catch (Exception e) {
                metricsByName = new HashMap<>();
            }
            return nodes.stream().map(node -> {
                NodeResponse r = new NodeResponse();
                r.setName(node.getMetadata().getName());
                r.setRoles(StringUtils.join(
                    node.getMetadata().getLabels().keySet().stream()
                        .filter(k -> k.startsWith("node-role.kubernetes.io/"))
                        .map(k -> k.substring("node-role.kubernetes.io/".length()))
                        .collect(Collectors.toList()), ","));
                r.setVersion(node.getStatus().getNodeInfo().getKubeletVersion());
                r.setStatus(isNodeReady(node) ? "Ready" : "NotReady");
                r.setInternalIp(node.getStatus().getAddresses().stream()
                    .filter(a -> "InternalIP".equals(a.getType()))
                    .map(NodeAddress::getAddress).findFirst().orElse(null));
                r.setCpuAllocatableMillis(
                    K8sQuantityUtils.cpuToMillis(node.getStatus().getAllocatable().get("cpu")));
                r.setMemAllocatableBytes(
                    K8sQuantityUtils.memoryToBytes(node.getStatus().getAllocatable().get("memory")));
                NodeMetrics m = metricsByName.get(node.getMetadata().getName());
                if (m != null) {
                    r.setCpuUsedMillis(K8sQuantityUtils.cpuToMillis(m.getUsage().get("cpu")));
                    r.setMemUsedBytes(K8sQuantityUtils.memoryToBytes(m.getUsage().get("memory")));
                }
                return r;
            }).collect(Collectors.toList());
        }
    }

    // ---------- deployments ----------
    @Override
    public List<DeploymentResponse> deployments(K8sCluster cluster, String namespace, String keyword) {
        try (KubernetesClient client = k8sClientManager.getOrCreate(cluster.getId(), cluster.getKubeconfig())) {
            List<Deployment> list = StringUtils.isBlank(namespace)
                ? client.apps().deployments().inAnyNamespace().list().getItems()
                : client.apps().deployments().inNamespace(namespace).list().getItems();
            Map<Long, String> appIndex = flinkAppIndex();  // (见 Task 6，先返回 emptyMap)
            return list.stream()
                .filter(d -> StringUtils.isBlank(keyword)
                    || d.getMetadata().getName().contains(keyword))
                .map(d -> {
                    DeploymentResponse r = new DeploymentResponse();
                    r.setName(d.getMetadata().getName());
                    r.setNamespace(d.getMetadata().getNamespace());
                    r.setReplicasDesired(d.getSpec().getReplicas() == null ? 0 : d.getSpec().getReplicas());
                    r.setReplicasReady(d.getStatus() == null || d.getStatus().getReadyReplicas() == null
                        ? 0 : d.getStatus().getReadyReplicas().intValue());
                    r.setCreationTimestamp(Date.from(d.getMetadata().getCreationTimestamp().toInstant()));
                    if (CollectionUtils.isNotEmpty(d.getSpec().getTemplate().getSpec().getContainers())) {
                        r.setImage(d.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
                    }
                    // streamPark annotation filled in Task 6
                    return r;
                })
                .collect(Collectors.toList());
        }
    }

    // ---------- pods ----------
    @Override
    public List<PodResponse> pods(K8sCluster cluster, String namespace, String keyword) {
        try (KubernetesClient client = k8sClientManager.getOrCreate(cluster.getId(), cluster.getKubeconfig())) {
            List<Pod> list = StringUtils.isBlank(namespace)
                ? client.pods().inAnyNamespace().list().getItems()
                : client.pods().inNamespace(namespace).list().getItems();
            Map<String, PodMetrics> podMetrics;
            try {
                podMetrics = client.top().pods().inAnyNamespace().list().getItems().stream()
                    .collect(Collectors.toMap(m -> m.getMetadata().getNamespace() + "/" + m.getMetadata().getName(),
                        m -> m, (a, b) -> a));
            } catch (Exception e) {
                podMetrics = new HashMap<>();
            }
            return list.stream()
                .filter(p -> StringUtils.isBlank(keyword)
                    || p.getMetadata().getName().contains(keyword))
                .map(p -> {
                    PodResponse r = new PodResponse();
                    r.setName(p.getMetadata().getName());
                    r.setNamespace(p.getMetadata().getNamespace());
                    r.setNodeName(p.getSpec().getNodeName());
                    r.setPhase(p.getStatus().getPhase());
                    r.setIp(p.getStatus().getPodIP());
                    r.setCreationTimestamp(Date.from(p.getMetadata().getCreationTimestamp().toInstant()));
                    if (p.getStatus().getContainerStatuses() != null) {
                        r.setRestarts(p.getStatus().getContainerStatuses().stream()
                            .mapToLong(s -> s.getRestartCount() == null ? 0 : s.getRestartCount().longValue())
                            .sum());
                    }
                    PodMetrics m = podMetrics.get(r.getNamespace() + "/" + r.getName());
                    if (m != null) {
                        r.setCpuMillis(m.getContainers().stream()
                            .mapToLong(c -> K8sQuantityUtils.cpuToMillis(c.getUsage().get("cpu"))).sum());
                        r.setMemoryBytes(m.getContainers().stream()
                            .mapToLong(c -> K8sQuantityUtils.memoryToBytes(c.getUsage().get("memory"))).sum());
                    }
                    return r;
                })
                .collect(Collectors.toList());
        }
    }

    // ---------- events ----------
    @Override
    public List<EventResponse> events(K8sCluster cluster, String namespace, int limit) {
        try (KubernetesClient client = k8sClientManager.getOrCreate(cluster.getId(), cluster.getKubeconfig())) {
            List<Event> list = StringUtils.isBlank(namespace)
                ? client.v1().events().inAnyNamespace().list().getItems()
                : client.v1().events().inNamespace(namespace).list().getItems();
            return list.stream()
                .filter(e -> "Warning".equals(e.getType()))
                .sorted(Comparator.comparing(Event::getLastTimestamp,
                    Comparator.nullsFirst(Comparator.naturalOrder())).reversed())
                .limit(limit)
                .map(e -> {
                    EventResponse r = new EventResponse();
                    r.setType(e.getType());
                    r.setReason(e.getReason());
                    r.setObject(e.getInvolvedObject().getKind() + "/" + e.getInvolvedObject().getName());
                    r.setMessage(e.getMessage());
                    r.setCount(e.getCount());
                    r.setLastTimestamp(e.getLastTimestamp() == null ? null : e.getLastTimestamp().toString());
                    return r;
                })
                .collect(Collectors.toList());
        }
    }

    // ---------- namespaces ----------
    @Override
    public List<String> namespaces(K8sCluster cluster) {
        try (KubernetesClient client = k8sClientManager.getOrCreate(cluster.getId(), cluster.getKubeconfig())) {
            return client.namespaces().list().getItems().stream()
                .map(Namespace::getMetadata).map(io.fabric8.kubernetes.api.model.ObjectMeta::getName)
                .collect(Collectors.toList());
        }
    }

    // ---------- private ----------
    private boolean isNodeReady(Node node) {
        if (node.getStatus().getConditions() == null) {
            return false;
        }
        return node.getStatus().getConditions().stream()
            .anyMatch(c -> "Ready".equals(c.getType()) && "True".equals(c.getStatus()));
    }

    private Map<Long, String> flinkAppIndex() {
        return Maps.newHashMap(); // Task 6 replaces
    }
}
```

注意：
- `try-with-resources` 关闭共享 client 会把缓存实例关掉——**改为不关闭**：K8sClientManager.getOrCreate 返回缓存实例，调用处不 close（client 生命周期由 manager 管理）。把上面所有 `try (...)` 改成普通块。
- `client.top().pods()` 在 fabric8 6.x 的真实签名是 `client.top().pods(String namespace)` 与 `client.top().pods()`（全 namespace），编译期确认。
- `Event::getLastTimestamp` 是 `String`（core/v1 Event 的 lastTimestamp 为 String 类型），排序按字符串倒序即可（ISO8601 字典序=时间序）。

- [ ] **Step 3: 实现 K8sMonitorController**

```java
package org.apache.streampark.console.kubernetes.controller;

import org.apache.streampark.console.base.domain.RestResponseBody;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.kubernetes.entity.K8sCluster;
import org.apache.streampark.console.kubernetes.response.ClusterOverviewResponse;
import org.apache.streampark.console.kubernetes.response.DeploymentResponse;
import org.apache.streampark.console.kubernetes.response.EventResponse;
import org.apache.streampark.console.kubernetes.response.NodeResponse;
import org.apache.streampark.console.kubernetes.response.PodResponse;
import org.apache.streampark.console.kubernetes.service.K8sClusterService;
import org.apache.streampark.console.kubernetes.service.K8sMonitorService;

import io.fabric8.kubernetes.client.KubernetesClientException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("k8s/monitor")
public class K8sMonitorController {

    @Autowired
    private K8sClusterService k8sClusterService;

    @Autowired
    private K8sMonitorService k8sMonitorService;

    @GetMapping("overview")
    public RestResponseBody<ClusterOverviewResponse> overview(@RequestParam Long clusterId) {
        return RestResponseBody.success(k8sMonitorService.options(cluster(), null));
    }

    // ... 其余端点同型：nodes / workloads(deployments) / pods / events / namespaces
    // events 参数 namespace + limit(默认50)；deployments/pods 参数 namespace、keyword

    private K8sCluster cluster(Long clusterId) {
        K8sCluster cluster = k8sClusterService.getById(clusterId);
        ApiAlertException.throwIfNull(cluster, "The kubernetes cluster registration doesn't exist.");
        return cluster;
    }
}
```

（执行时写完整 6 个端点：overview/nodes/workloads/pods/events/namespaces。把 KubernetesClientException 捕获后转成 `ApiAlertException("Kubernetes API error: " + msg)` 的包装逻辑放在 Service 入口或 Controller 统一 try-catch。）

- [ ] **Step 4: 写测试（fabric8 KubernetesServer mock）**

```java
package org.apache.streampark.console.kubernetes.service;

import org.apache.streampark.console.kubernetes.client.K8sClientManager;
import org.apache.streampark.console.kubernetes.entity.K8sCluster;
import org.apache.streampark.console.kubernetes.response.ClusterOverviewResponse;
import org.apache.streampark.console.kubernetes.response.NodeResponse;
import org.apache.streampark.console.kubernetes.service.impl.K8sMonitorServiceImpl;

import io.fabric8.kubernetes.api.model.NodeBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.server.mock.KubernetesServer;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

class K8sMonitorServiceTest {

    @Rule
    public KubernetesServer server = new KubernetesServer(false, true);

    private K8sMonitorServiceImpl monitorService;
    private K8sCluster cluster;

    @BeforeEach
    void setUp() {
        server.before();
        K8sClientManager manager = new K8sClientManager();
        K8sMonitorServiceImpl impl = new K8sMonitorServiceImpl();
        // 注入 manager 反射/字段注入——将 K8sMonitorServiceImpl 的 k8sClientManager 字段
        // 改为包级可见或提供 @VisibleForTesting setter，测试用例注入 manager 的 mock 版本：
        // manager.getClientFactory() 不可 mock —— 因此 K8sMonitorServiceImpl 需要允许替换 client 构造。
        // 方案：给 K8sMonitorServiceImpl 加 @VisibleForTesting setter setK8sClientManager()，
        // 测试里用一个重写的 K8sClientManager 子类（buildClient 返回 server.client）。
        K8sClientManager mockManager = new K8sClientManager() {
            @Override
            public io.fabric8.kubernetes.client.KubernetesClient buildClient(String kubeconfig) {
                return server.getClient();
            }
        };
        impl.setK8sClientManager(mockManager);
        monitorService = impl;
        cluster = new K8sCluster();
        cluster.setId(1L);
        cluster.setKubeconfig("fake");
    }

    @Test
    void testOverviewAndNodes() {
        server.expect().withPath("/api/v1/nodes").andReturn(200, new io.fabric8.kubernetes.api.model.NodeListBuilder()
            .addNewItem().withMetadata(new ObjectMetaBuilder().withName("node-1").build())
            .withStatus(new io.fabric8.kubernetes.api.model.NodeStatusBuilder()
                .withNodeInfo(new io.fabric8.kubernetes.api.model.NodeSystemInfoBuilder()
                    .withKubeletVersion("v1.29.0").build())
                .withConditions(new io.fabric8.kubernetes.api.model.NodeConditionBuilder()
                    .withType("Ready").withStatus("True").build())
                .build())
            .endItem().build()).always();

        ClusterOverviewResponse overview = monitorService.overview(cluster);
        assertThat(overview.getNodesTotal()).isEqualTo(1L);
        assertThat(overview.getNodesReady()).isEqualTo(1L);

        NodeResponse node = monitorService.nodes(cluster).get(0);
        assertThat(node.getName()).isEqualTo("node-1");
        assertThat(node.getStatus()).isEqualTo("Ready");
    }
}
```

注意：mock server 未模拟 metrics API，overview 的 metrics 部分应走 catch 分支（metricsServerAvailable=false）——这正是降级路径测试。若 `@Rule` 与 JUnit5 不兼容，改用 `@RegisterExtension`（fabric8 kubernetes-server-mock 的 KubernetesServer 实现了 TestRule，JUnit5 需要 `@Rule` 兼容层或直接管理生命周期：`server.before()` / `server.after()` 手动调用即可，推荐手动 before/after 写法）。Mockito import 不需要则删。

- [ ] **Step 5: 运行测试**

Run: `mvn -q -pl streampark-console/streampark-console-service test -Dtest=K8sMonitorServiceTest`
Expected: PASS

- [ ] **Step 6: 编译 + Commit**

Run: `mvn -q -pl streampark-console/streampark-console-service -am compile -DskipTests`
Expected: BUILD SUCCESS

```bash
git add -A streampark-console/streampark-console-service/src
git commit -m "[K8s] Add cluster realtime monitoring APIs (overview/nodes/workloads/events)"
```

---

### Task 5: Prometheus 历史（K8sPrometheusClient + history 端点）

**Files:**
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/client/K8sPrometheusClient.java`
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/response/HistoryResponse.java`
- Modify: `K8sMonitorService` / `K8sMonitorServiceImpl` / `K8sMonitorController`（history）
- Test: `streampark-console/streampark-console-service/src/test/java/org/apache/streampark/console/kubernetes/client/K8sPrometheusClientTest.java`

- [ ] **Step 1: 写失败测试（纯解析逻辑，不打真实 HTTP）**

```java
package org.apache.streampark.console.kubernetes.client;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class K8sPrometheusClientTest {

    private static final String MATRIX_JSON =
        "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[{\"metric\":{},"
            + "\"values\":[[1700000000,\"1.5\"],[1700000060,\"2.5\"]]}]}}";

    @Test
    void testParseMatrix() {
        List<long[]> series = K8sPrometheusClient.parseMatrix(MATRIX_JSON);
        assertThat(series).hasSize(2);
        assertThat(series.get(0)).containsExactly(1700000000L, 1.5d);
        assertThat(series.get(1)).containsExactly(1700000060L, 2.5d);
    }

    @Test
    void testBuildRangeUrl() {
        String url = K8sPrometheusClient.buildRangeUrl(
            "http://prom:9090", "sum(rate(container_cpu_usage_seconds_total[5m]))", 1000L, 2000L, 60);
        assertThat(url).startsWith("http://prom:9090/api/v1/query_range?query=sum%28rate%28container_cpu")
            .contains("start=1000").contains("end=2000").contains("step=60");
    }
}
```

- [ ] **Step 2: 运行确认失败 → 实现 K8sPrometheusClient**

```java
package org.apache.streampark.console.kubernetes.client;

import org.apache.streampark.common.util.HttpClientUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Minimal Prometheus HTTP API client (query_range only). */
public final class K8sPrometheusClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private K8sPrometheusClient() {
    }

    public static String buildRangeUrl(
        String prometheusUrl, String query, long startSec, long endSec, int stepSec) {
        return prometheusUrl.replaceAll("/$", "") + "/api/v1/query_range?query="
            + URLEncoder.encode(query, StandardCharsets.UTF_8)
            + "&start=" + startSec + "&end=" + endSec + "&step=" + stepSec;
    }

    /** Parses a matrix result into [timestampSec, value] pairs (first series only). */
    public static List<long[]> parseMatrix(String body) {
        List<long[]> points = new ArrayList<>();
        try {
            JsonNode data = MAPPER.readTree(body).path("data").path("result");
            if (data.isArray() && data.size() > 0) {
                for (JsonNode v : data.get(0).path("values")) {
                    long ts = v.get(0).asLong();
                    double val = Double.parseDouble(v.get(1).asText());
                    points.add(new long[] {ts, (long) val});
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse prometheus response: " + e.getMessage(), e);
        }
        return points;
    }

    /** Executes query_range and returns parsed series. */
    public static List<long[]> queryRange(
        String prometheusUrl, String query, long startSec, long endSec, int stepSec) {
        String url = buildRangeUrl(prometheusUrl, query, startSec, endSec, stepSec);
        return parseMatrix(HttpClientUtils.httpGetRequest(url));
    }
}
```

`long[]` 装载 double 值有精度损失（CPU millis / bytes 都是 long 量级，无问题）。`HttpClientUtils.httpGetRequest(String)` 单参版本存在（已核实 L90 区域有多个重载）。

- [ ] **Step 3: 运行测试通过**

Run: `mvn -q -pl streampark-console/streampark-console-service test -Dtest=K8sPrometheusClientTest`
Expected: PASS

- [ ] **Step 4: HistoryResponse + Service 方法 + 端点**

HistoryResponse：

```java
private List<long[]> cpuSeries;   // [tsSec, cpuMillis]
private List<long[]> memSeries;   // [tsSec, bytes]
```

ServiceImpl 加：

```java
@Override
public HistoryResponse history(K8sCluster cluster, String kind, String name, String namespace, String range) {
    ApiAlertException.throwIfTrue(
        StringUtils.isBlank(cluster.getPrometheusUrl()),
        "Prometheus is not configured for this cluster.");
    RangeSpec spec = parseRange(range);   // "1h"->3600 etc: 1h/6h/24h/7d
    long end = System.currentTimeMillis() / 1000;
    long start = end - spec.seconds;
    int step = Math.max(spec.seconds / 60, 30);   // ~60 points
    String cpuQuery;
    String memQuery;
    switch (kind) {
        case "node":
            cpuQuery = String.format(
                "sum(rate(container_cpu_usage_seconds_total{node=\"%s\"}[5m])) * 1000", name);
            memQuery = String.format(
                "sum(container_memory_working_set_bytes{node=\"%s\"})", name);
            break;
        case "pod":
            cpuQuery = String.format(
                "sum(rate(container_cpu_usage_seconds_total{namespace=\"%s\",pod=\"%s\"}[5m])) * 1000",
                namespace, name);
            memQuery = String.format(
                "sum(container_memory_working_set_bytes{namespace=\"%s\",pod=\"%s\"})",
                namespace, name);
            break;
        default: // cluster
            cpuQuery = "sum(rate(container_cpu_usage_seconds_total{container!=\"\",container!=\"POD\"}[5m])) * 1000";
            memQuery = "sum(container_memory_working_set_bytes{container!=\"\",container!=\"POD\"})";
            break;
    }
    HistoryResponse resp = new HistoryResponse();
    resp.setCpuSeries(K8sPrometheusClient.queryRange(
        cluster.getPrometheusUrl(), cpuQuery, start, end, step));
    resp.setMemSeries(K8sPrometheusClient.queryRange(
        cluster.getPrometheusUrl(), memQuery, start, end, step));
    return resp;
}

// RangeSpec 简单 record：Map.of("1h",3600L, "6h",21600L, "24h",86400L, "7d",604800L)
```

Controller 加：

```java
@GetMapping("history")
public RestResponseBody<HistoryResponse> history(
    @RequestParam Long clusterId, @RequestParam(defaultValue = "cluster") String kind,
    @RequestParam(required = false) String name,
    @RequestParam(required = false) String namespace,
    @RequestParam(defaultValue = "6h") String range) {
    return RestResponseBody.success(
        k8sMonitorService.history(cluster(clusterId), kind, name, namespace, range));
}
```

PromQL 依赖 kubelet cAdvisor 指标（kube-prometheus-stack 标准配置，`node`/`pod` label 可用）。前端趋势区需注明该前置条件。

- [ ] **Step 5: Commit**

```bash
git add -A streampark-console/streampark-console-service/src
git commit -m "[K8s] Add optional Prometheus history trends for cluster/node/pod"
```

---

### Task 6: StreamPark Flink 应用标注 + Flink 应用清单端点

**Files:**
- Create: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/kubernetes/response/FlinkAppResponse.java`
- Modify: `K8sMonitorServiceImpl`（deployments 标注 + flinkApps 方法）
- Modify: `K8sMonitorService` / `K8sMonitorController`（flinkApps 端点）
- Test: `streampark-console/streampark-console-service/src/test/java/org/apache/streampark/console/kubernetes/service/K8sFlinkAppIndexTest.java`

- [ ] **Step 1: 实现 FlinkAppResponse**

```java
private Long id;               // FlinkApplication.id
private String appName;        // FlinkApplication.jobName（K8S deployment 名）
private String k8sNamespace;
private String jobState;       // watcher FlinkJobState.name()，可能为 null
private Integer deployMode;    // 5=session 6=application
private Long cpuMillis;        // 关联 Pod 实时值（选中的 k8s 集群内匹配到时）
private Long memoryBytes;
private Long restarts;
private String podPhase;
```

- [ ] **Step 2: 实现 deployIndex 构建（可单测的纯静态方法）**

```java
/** index: k8sNamespace + "/" + jobName -> appId，仅 K8S 模式应用。 */
@VisibleForTesting
public static Map<String, Long> buildDeployIndex(List<FlinkApplication> apps) {
    Map<String, Long> index = new HashMap<>();
    if (apps != null) {
        for (FlinkApplication app : apps) {
            FlinkDeployMode mode = FlinkDeployMode.of(app.getDeployMode());
            if (FlinkDeployMode.isKubernetesMode(mode)
                && StringUtils.isNotBlank(app.getJobName())) {
                index.put(app.getK8sNamespace() + "/" + app.getJobName(), app.getId());
            }
        }
    }
    return index;
}
```

- [ ] **Step 3: deployments() 中应用标注**

```java
Map<Long, FlinkApplication> appsById = loadK8sApps().stream()
    .collect(Collectors.toMap(FlinkApplication::getId, a -> a));
// 循环内：
Long appId = buildDeployIndex(loadK8sApps()).get(r.getNamespace() + "/" + r.getName());
if (appId != null) {
    FlinkApplication app = appsById.get(appId);
    r.setStreamParkAppId(appId);
    r.setStreamParkAppName(app.getJobName());
}
```

（实现时把 loadK8sApps 结果一次查出传两个用途，避免重复查询。）

loadK8sApps：

```java
private List<FlinkApplication> loadK8sApps() {
    return flinkApplicationMapper.selectList(
        new LambdaQueryWrapper<FlinkApplication>()
            .in(FlinkApplication::getDeployMode,
                FlinkDeployMode.KUBERNETES_NATIVE_SESSION.getValue(),
                FlinkDeployMode.KUBERNETES_NATIVE_APPLICATION.getValue()));
}
```

需要 `FlinkApplicationMapper` 注入（`org.apache.streampark.console.core.mapper.FlinkApplicationMapper`，BaseMapper）与 `FlinkDeployMode.of(Integer)` / `getValue()`——先 grep 确认 FlinkDeployMode 的值读取方法名（可能是 `get()`/`getValue()`/`of(value)` 反查），按实际 API 写。

- [ ] **Step 4: flinkApps() 端点（watcher 状态 + 选中集群 Pod 用量合并）**

```java
@Override
public List<FlinkAppResponse> flinkApps(K8sCluster cluster) {
    List<FlinkApplication> apps = loadK8sApps();
    Map<CacheKey, JobStatusCV> jobStatuses = flinkK8sWatcher.getAllJobStatus();
    Map<Long, JobStatusCV> statusByAppId = jobStatuses == null ? new HashMap<>()
        : jobStatuses.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().key(), Map.Entry::getValue, (a, b) -> a));

    // pod 用量（可选项：该 app 的 deployment 若落在当前选中集群里）
    Map<String, PodResponse> podByDeploy;
    try {
        podByDeploy = pods(cluster, null, null).stream()
            .filter(p -> p.getNamespace() != null && p.getName() != null)
            .collect(Collectors.toMap(
                p -> p.getNamespace() + "/" + deploymentNameOf(p.getName()), p -> p, (a, b) -> a));
    } catch (Exception e) {
        podByDeploy = new HashMap<>();
    }

    return apps.stream().map(app -> {
        FlinkAppResponse r = new FlinkAppResponse();
        r.setId(app.getId());
        r.setAppName(app.getJobName());
        r.setK8sNamespace(app.getK8sNamespace());
        r.setDeployMode(app.getDeployMode());
        JobStatusCV status = statusByAppId.get(app.getId());
        if (status != null && status.jobState() != null) {
            r.setJobState(status.jobState().name());
        }
        PodResponse pod = podByDeploy.get(r.getK8sNamespace() + "/" + r.getAppName());
        if (pod != null) {
            r.setCpuMillis(pod.getCpuMillis());
            r.setMemoryBytes(pod.getMemoryBytes());
            r.setRestarts(pod.getRestarts());
            r.setPodPhase(pod.getPhase());
        }
        return r;
    }).collect(Collectors.toList());
}

// Flink 原生 K8S pod 名形如 <deployment>-<hash>-<id>，deploymentNameOf 取前两段：
private static String deploymentNameOf(String podName) {
    String[] parts = podName.split("-");
    return parts.length <= 2 ? podName : parts[0] + "-" + parts[1];
}
```

注入：`FlinkKubernetesWatcher flinkK8sWatcher`（`org.apache.streampark.flink.kubernetes.FlinkKubernetesWatcher`，console 已有 @Bean）。

注意：StreamPark 提交的 Flink deployment 名与 pod 名生成规则由 Flink kubernetes 模块决定（`<cluster-id>`-hash），`deploymentNameOf` 用「前两段拼接」是近似匹配，若实际是 `<name>-<hash>` 两段式需调整——执行时以 `FlinkApplication.jobName` 实际生成的 deployment 名规则核对（可在 FlinkK8sWatcher 的 k8sDeploymentEvents 数据结构中确认命名），不匹配就改为 label 匹配（pod label `app=<jobName>`）。

- [ ] **Step 5: Controller 端点**

```java
@GetMapping("flinkapps")
public RestResponseBody<List<FlinkAppResponse>> flinkApps(@RequestParam Long clusterId) {
    return RestResponseBody.success(k8sMonitorService.flinkApps(cluster(clusterId)));
}
```

- [ ] **Step 6: 写 buildDeployIndex 单测**

```java
package org.apache.streampark.console.kubernetes.service;

import org.apache.streampark.common.enums.FlinkDeployMode;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.kubernetes.service.impl.K8sMonitorServiceImpl;

import org.junit.jupiter.api.Test;

import java.util.List;
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
    void testIndexOnlyK8sApps() {
        Map<String, Long> index = K8sMonitorServiceImpl.buildDeployIndex(
            List.of(
                app(1L, "default", "app-one", FlinkDeployMode.KUBERNETES_NATIVE_APPLICATION.getValue()),
                app(2L, "flink-ns", "app-two", FlinkDeployMode.YARN_SESSION.getValue())));
        assertThat(index).containsEntry("default/app-one", 1L).hasSize(1);
    }
}
```

（`FlinkDeployMode.YARN_SESSION.getValue()` 的方法名以实际为准——grep `enum FlinkDeployMode` 的字段访问器。）

- [ ] **Step 7: 运行测试 + 编译 + Commit**

Run: `mvn -q -pl streampark-console/streampark-console-service test -Dtest='K8sFlinkAppIndexTest'`
Expected: PASS

```bash
git add -A streampark-console/streampark-console-service/src
git commit -m "[K8s] Annotate StreamPark-managed Flink deployments and add flink apps endpoint"
```

---

### Task 7: 菜单数据（mysql/pgsql data.sql + t_role_menu）

**Files:**
- Modify: `streampark-console/streampark-console-service/src/main/assembly/script/data/mysql-data.sql`
- Modify: `streampark-console/streampark-console-service/src/main/assembly/script/data/pgsql-data.sql`

- [ ] **Step 1: 确认 ID 段空闲**

Run: `grep -n "16[0-9]000" streampark-console/streampark-console-service/src/main/assembly/script/data/mysql-data.sql`
Expected: 无输出（160000 段空闲；若被占用顺延到下一空闲段）

- [ ] **Step 2: mysql-data.sql 追加（文件末尾，t_role_menu 区域之后或文件末尾）**

```sql
-- ----------------------------
-- Kubernetes menus
-- ----------------------------
insert into `t_menu` values (160000, 0, 'menu.kubernetes', '/kubernetes', 'PageView', null, 0, '0', 1, 6, now(), now());
insert into `t_menu` values (160100, 160000, 'menu.kubernetes.cluster', '/kubernetes/cluster', 'kubernetes/cluster/View', null, null, '0', 1, 1, now(), now());
insert into `t_menu` values (160101, 160100, 'k8s cluster add', null, null, 'k8sCluster:create', null, '1', 1, null, now(), now());
insert into `t_menu` values (160102, 160100, 'k8s cluster edit', null, null, 'k8sCluster:update', null, '1', 1, null, now(), now());
insert into `t_menu` values (160103, 160100, 'k8s cluster delete', null, null, 'k8sCluster:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (160200, 160000, 'menu.kubernetes.monitor', '/kubernetes/monitor', 'kubernetes/monitor/index', null, null, '0', 1, 2, now(), now());

insert into `t_role_menu` (role_id, menu_id) values (100001, 160000);
insert into `t_role_menu` (role_id, menu_id) values (100001, 160100);
insert into `t_role_menu` (role_id, menu_id) values (100001, 160101);
insert into `t_role_menu` (role_id, menu_id) values (100001, 160102);
insert into `t_role_menu` (role_id, menu_id) values (100001, 160103);
insert into `t_role_menu` (role_id, menu_id) values (100001, 160200);
insert into `t_role_menu` (role_id, menu_id) values (100002, 160000);
insert into `t_role_menu` (role_id, menu_id) values (100002, 160100);
insert into `t_role_menu` (role_id, menu_id) values (100002, 160101);
insert into `t_role_menu` (role_id, menu_id) values (100002, 160102);
insert into `t_role_menu` (role_id, menu_id) values (100002, 160103);
insert into `t_role_menu` (role_id, menu_id) values (100002, 160200);
```

对照该文件 t_menu 列序（id, parent_id, name, path, component, perm, icon?, type, order...）——以文件内 140400 行的实际列数为准确认；列数不一致时按实际调整 values。

- [ ] **Step 3: pgsql-data.sql 镜像追加（读文件确认语法后镜像；pgsql 与 mysql 的 t_menu 列定义一致）**

- [ ] **Step 4: Commit**

```bash
git add streampark-console/streampark-console-service/src/main/assembly/script/data/
git commit -m "[K8s] Add kubernetes menus (cluster management + monitoring) to init data"
```

---

### Task 8: 前端依赖 + API 模块 + 类型

**Files:**
- Modify: `streampark-console/streampark-console-webapp/package.json`（dependencies 加 `"echarts": "^5.5.1"`）
- Create: `streampark-console/streampark-console-webapp/src/api/kubernetes/cluster.ts`
- Create: `streampark-console/streampark-console-webapp/src/api/kubernetes/cluster.type.ts`
- Create: `streampark-console/streampark-console-webapp/src/api/kubernetes/monitor.ts`
- Create: `streampark-console/streampark-console-webapp/src/api/kubernetes/monitor.type.ts`

- [ ] **Step 1: package.json 加 echarts，然后**

Run: `cd streampark-console/streampark-console-webapp && pnpm install`
Expected: 依赖安装成功

- [ ] **Step 2: cluster.type.ts**

```ts
export interface K8sCluster {
  id: string | number;
  clusterName: string;
  prometheusUrl?: string;
  description?: string;
  createTime?: string;
  modifyTime?: string;
}

export interface K8sClusterTestResult {
  connected: boolean;
  version?: string;
  message?: string;
}
```

- [ ] **Step 3: cluster.ts（模式仿 api/setting/yarnQueue.ts）**

```ts
import { Result } from '/#/axios';
import { AxiosResponse } from 'axios';
import { defHttp } from '/@/utils/http/axios';
import type { K8sCluster, K8sClusterTestResult } from './cluster.type';

enum K8S_CLUSTER_API {
  LIST = '/k8s/cluster/list',
  CREATE = '/k8s/cluster/create',
  UPDATE = '/k8s/cluster/update',
  DELETE = '/k8s/cluster/delete',
  TEST = '/k8s/cluster/test',
}

export function fetchK8sClusterList() {
  return defHttp.post<K8sCluster[]>({ url: K8S_CLUSTER_API.LIST });
}

export function fetchK8sClusterCreate(data: Recordable) {
  return defHttp.post({ url: K8S_CLUSTER_API.CREATE, data });
}

export function fetchK8sClusterUpdate(data: Recordable) {
  return defHttp.post({ url: K8S_CLUSTER_API.UPDATE, data });
}

export function fetchK8sClusterDelete(id: number | string): Promise<AxiosResponse<Result>> {
  return defHttp.post(
    { url: K8S_CLUSTER_API.DELETE, data: { id } },
    { isReturnNativeResponse: true },
  );
}

export function fetchK8sClusterTest(data: Recordable) {
  return defHttp.post<K8sClusterTestResult>({ url: K8S_CLUSTER_API.TEST, data });
}
```

- [ ] **Step 4: monitor.type.ts**

```ts
export interface ClusterOverview {
  nodesTotal: number;
  nodesReady: number;
  podsRunning: number;
  podsPending: number;
  podsFailed: number;
  namespaces: number;
  cpuAllocatableMillis: number;
  cpuUsedMillis: number;
  memAllocatableBytes: number;
  memUsedBytes: number;
  warningEvents?: number;
  metricsServerAvailable: boolean;
}

export interface K8sNode {
  name: string;
  roles?: string;
  version?: string;
  status: string;
  internalIp?: string;
  podsCount?: number;
  cpuAllocatableMillis: number;
  cpuUsedMillis?: number;
  memAllocatableBytes: number;
  memUsedBytes?: number;
}

export interface K8sDeployment {
  name: string;
  namespace: string;
  replicasReady: number;
  replicasDesired: number;
  image?: string;
  creationTimestamp?: string;
  streamParkAppId?: number;
  streamParkAppName?: string;
}

export interface K8sPod {
  name: string;
  namespace: string;
  nodeName?: string;
  phase: string;
  restarts?: number;
  cpuMillis?: number;
  memoryBytes?: number;
  ip?: string;
  creationTimestamp?: string;
}

export interface K8sEvent {
  type?: string;
  reason?: string;
  object?: string;
  message?: string;
  count?: number;
  lastTimestamp?: string;
}

export interface FlinkAppOnK8s {
  id: number | string;
  appName: string;
  k8sNamespace: string;
  jobState?: string;
  deployMode: number;
  cpuMillis?: number;
  memoryBytes?: number;
  restarts?: number;
  podPhase?: string;
}

export interface HistorySeries {
  cpuSeries: [number, number][];
  memSeries: [number, number][];
}
```

- [ ] **Step 5: monitor.ts**

```ts
import { defHttp } from '/@/utils/http/axios';
import type {
  ClusterOverview,
  FlinkAppOnK8s,
  HistorySeries,
  K8sDeployment,
  K8sEvent,
  K8sNode,
  K8sPod,
} from './monitor.type';

enum K8S_MONITOR_API {
  OVERVIEW = '/k8s/monitor/overview',
  NODES = '/k8s/monitor/nodes',
  WORKLOADS = '/k8s/monitor/workloads',
  PODS = '/k8s/monitor/pods',
  EVENTS = '/k8s/monitor/events',
  NAMESPACES = '/k8s/monitor/namespaces',
  HISTORY = '/k8s/monitor/history',
  FLINK_APPS = '/k8s/monitor/flinkapps',
}

const get = <T>(url: string, params: Recordable) => defHttp.get<T>({ url, params });

export const fetchK8sOverview = (clusterId: number | string) =>
  get<ClusterOverview>(K8S_MONITOR_API.OVERVIEW, { clusterId });
export const fetchK8sNodes = (clusterId: number | string) =>
  get<K8sNode[]>(K8S_MONITOR_API.NODES, { clusterId });
export const fetchK8sDeployments = (clusterId: number | string, namespace?: string, keyword?: string) =>
  get<K8sDeployment[]>(K8S_MONITOR_API.WORKLOADS, { clusterId, namespace, keyword });
export const fetchK8sPods = (clusterId: number | string, namespace?: string, keyword?: string) =>
  get<K8sPod[]>(K8S_MONITOR_API.PODS, { clusterId, namespace, keyword });
export const fetchK8sEvents = (clusterId: number | string, namespace?: string, limit = 50) =>
  get<K8sEvent[]>(K8S_MONITOR_API.EVENTS, { clusterId, namespace, limit });
export const fetchK8sNamespaces = (clusterId: number | string) =>
  get<string[]>(K8S_MONITOR_API.NAMESPACES, { clusterId });
export const fetchK8sHistory = (
  clusterId: number | string,
  kind: 'cluster' | 'node' | 'pod',
  name?: string,
  namespace?: string,
  range = '6h',
) => get<HistorySeries>(K8S_MONITOR_API.HISTORY, { clusterId, kind, name, namespace, range });
export const fetchK8sFlinkApps = (clusterId: number | string) =>
  get<FlinkAppOnK8s[]>(K8S_MONITOR_API.FLINK_APPS, { clusterId });
```

- [ ] **Step 6: Commit**

```bash
git add streampark-console/streampark-console-webapp/package.json streampark-console/streampark-console-webapp/pnpm-lock.yaml streampark-console/streampark-console-webapp/src/api/kubernetes/
git commit -m "[K8s] Add frontend echarts dependency and kubernetes api modules"
```

---

### Task 9: 前端 i18n + 集群管理页面

**Files:**
- Create: `streampark-console/streampark-console-webapp/src/locales/lang/zh-CN/kubernetes.ts`
- Create: `streampark-console/streampark-console-webapp/src/locales/lang/en/kubernetes.ts`
- Modify: `src/locales/lang/{zh-CN,en}/index.ts`（注册模块，仿 yarnQueue 的注册方式——先读该文件确认）
- Modify: `src/locales/lang/{zh-CN,en}/menu.ts`（加 `kubernetes` 段）
- Create: `src/views/kubernetes/cluster/index.data.ts`
- Create: `src/views/kubernetes/cluster/ClusterModal.vue`
- Create: `src/views/kubernetes/cluster/View.vue`

- [ ] **Step 1: menu.ts（zh-CN 与 en 同步）**

```ts
kubernetes: {
  menu: 'Kubernetes',
  cluster: 'K8S 集群',
  monitor: 'K8S 监控',
},
// en:
kubernetes: {
  menu: 'Kubernetes',
  cluster: 'K8S Clusters',
  monitor: 'K8S Monitor',
},
```

（menu 键名与 t_menu.name 对应：`menu.kubernetes` / `menu.kubernetes.cluster` / `menu.kubernetes.monitor` —— 先读 menu.ts 现有结构确认嵌套约定，t_menu.name 使用 `menu.kubernetes` 这种点号键对应 `kubernetes: { menu: ... }` 结构；若现有是 `kubernetes: { menu: ... }` 则 t_menu.name 写 `menu.kubernetes`。）

- [ ] **Step 2: kubernetes.ts locale 模块（两语言）**

```ts
export default {
  kubernetes: {
    cluster: {
      create: '新建集群',
      edit: '编辑集群',
      name: '集群名称',
      kubeconfig: 'Kubeconfig',
      kubeconfigTip: '粘贴 kubeconfig 文件内容',
      prometheusUrl: 'Prometheus 地址',
      prometheusUrlTip: '选填，配置后可查看历史趋势',
      description: '描述',
      test: '测试连接',
      testOk: '连接成功',
      testFail: '连接失败',
      deleteConfirm: '确定删除该 K8S 集群注册吗？',
    },
    monitor: {
      overview: '总览',
      nodes: '节点',
      workloads: '工作负载',
      flinkApps: 'Flink 应用',
      clusters: '集群',
      autoRefresh: '自动刷新',
      metricsMissing: 'metrics-server 未安装，无法展示资源用量',
      cpu: 'CPU',
      memory: '内存',
      allocatable: '可分配',
      used: '使用量',
      trend: '历史趋势',
      prometheusMissing: '未配置 Prometheus，无法查看历史趋势',
      streamParkManaged: 'StreamPark 管理',
      ready: '就绪',
      pods: 'Pod 数',
      warningEvents: '警告事件',
      namespace: '命名空间',
      keyword: '关键字',
      search: '搜索',
      deployments: 'Deployments',
      podsTitle: 'Pods',
      type: '类型',
      range: '时间范围',
    },
  },
};
```

en 版本翻译同结构。index.ts 注册方式先读现有文件（zh-CN/index.ts）。

- [ ] **Step 3: cluster/index.data.ts**

```ts
import { useI18n } from '/@/hooks/web/useI18n';
import { FormSchema } from '/@/components/Form';
import { BasicColumn } from '/@/components/Table';

export const columns: BasicColumn[] = [
  { title: 'ID', dataIndex: 'id', width: 70 },
  { title: 'Name', dataIndex: 'clusterName' },
  { title: 'Prometheus', dataIndex: 'prometheusUrl' },
  { title: 'Description', dataIndex: 'description' },
  { title: 'Create Time', dataIndex: 'createTime', width: 160 },
];

export const formSchema = (): FormSchema[] => {
  const { t } = useI18n();
  return [
    { field: 'clusterName', label: t('kubernetes.cluster.name'), required: true, component: 'Input' },
    {
      field: 'kubeconfig',
      label: t('kubernetes.cluster.kubeconfig'),
      required: true,
      component: 'InputTextArea',
      componentProps: { rows: 10, placeholder: t('kubernetes.cluster.kubeconfigTip') },
    },
    {
      field: 'prometheusUrl',
      label: t('kubernetes.cluster.prometheusUrl'),
      component: 'Input',
      componentProps: { placeholder: t('kubernetes.cluster.prometheusUrlTip') },
    },
    { field: 'description', label: t('kubernetes.cluster.description'), component: 'InputTextArea', componentProps: { rows: 2 } },
  ];
};
```

- [ ] **Step 4: ClusterModal.vue（仿 YarnQueueModal.vue 结构：BasicModal + Form + 测试连接按钮）**

模板结构（完整实现参照 `views/setting/yarn-queue/YarnQueueModal.vue` 的 useModalInner/useForm 模式）：

- 表单字段来自 formSchema()；编辑模式回填（isUpdate 时 setFieldsValue，kubeconfig 可选回填）
- 底部按钮：测试连接（调 fetchK8sClusterTest({kubeconfig: 当前值})，结果 createMessage.success/error）、提交（create/update）
- 提交成功 emit('success') + closeModal

- [ ] **Step 5: cluster/View.vue（仿 yarn-queue/View.vue：BasicTable + Modal，无分页 api 用 fetchK8sClusterList）**

- useTable({ api: fetchK8sClusterList, columns, pagination: false, ... })
- 操作列：编辑/删除（v-auth: k8sCluster:update / k8sCluster:delete）
- 头部新建按钮 v-auth="'k8sCluster:create'"
- 删除走 fetchK8sClusterDelete + reload

- [ ] **Step 6: 验证**

Run: `cd streampark-console/streampark-console-webapp && pnpm build`（或先 `pnpm type:check` 快速验证 TS）
Expected: 无编译错误（页面未挂菜单前不可见属正常）

- [ ] **Step 7: Commit**

```bash
git add streampark-console/streampark-console-webapp/src/locales streampark-console/streampark-console-webapp/src/views/kubernetes
git commit -m "[K8s] Add kubernetes cluster management page"
```

---

### Task 10: 监控页外壳 + 总览 Tab

**Files:**
- Create: `src/views/kubernetes/monitor/index.vue`
- Create: `src/views/kubernetes/monitor/tabs/OverviewTab.vue`
- Create: `src/views/kubernetes/monitor/components/ResourceRingChart.vue`

- [ ] **Step 1: index.vue 外壳**

要点：
- 顶部一行：`a-select` 集群切换（fetchK8sClusterList；选中持久化在 ref），自动刷新开关（a-switch）+ 10s 定时器（onUnmounted 清理），手动刷新按钮
- `a-tabs`：overview / nodes / workloads / flinkApps（组件 lazy 挂载：activeTab 变化才加载对应 tab 内容，v-if 控制）
- `provide('clusterId', ...)` 或 props 传 clusterId 给各 tab
- 所有 tab 公共轮询逻辑放 index（暴露 reloadKey ref，子组件 watch）

- [ ] **Step 2: OverviewTab.vue**

要点：
- `a-row` 统计卡（Statistic）：节点 Ready `nodesReady/nodesTotal`、运行 Pod `podsRunning`、CPU 分配率、内存分配率、命名空间数
- ResourceRingChart 两个：CPU（used/allocatable，metrics 缺失时显示 metricsMissing 提示）、内存
- `metricsServerAvailable === false` 时 `a-alert` 警示条 t('kubernetes.monitor.metricsMissing')
- watch clusterId + reloadKey 拉数据（fetchK8sOverview）

- [ ] **Step 3: ResourceRingChart.vue**

```vue
<template>
  <div ref="chartRef" :style="{ height: '180px', width: '100%' }"></div>
</template>
<script lang="ts" setup>
  import * as echarts from 'echarts';
  import { onBeforeUnmount, onMounted, ref, watch } from 'vue';

  const props = defineProps<{ title: string; used: number; total: number; formatter: (v: number) => string }>();

  const chartRef = ref<HTMLDivElement>();
  let chart: echarts.ECharts | null = null;

  const render = () => {
    if (!chartRef.value) return;
    if (!chart) chart = echarts.init(chartRef.value);
    const pct = props.total > 0 && props.used >= 0 ? Math.round((props.used / props.total) * 100) : 0;
    chart.setOption({
      title: { text: props.title, left: 'center', textStyle: { fontSize: 13 } },
      tooltip: { formatter: `{b}: ${props.formatter(props.used)} / ${props.formatter(props.total)}` },
      series: [
        {
          type: 'pie',
          radius: ['55%', '75%'],
          label: { show: true, position: 'center', formatter: `${pct}%` },
          data: [
            { value: Math.max(props.used, 0), name: 'Used', itemStyle: { color: '#3b82f6' } },
            { value: Math.max(props.total - Math.max(props.used, 0), 0), name: 'Free', itemStyle: { color: '#e5e7eb' } },
          ],
        },
      ],
    });
  };

  watch(() => [props.used, props.total], render);
  onMounted(render);
  onBeforeUnmount(() => {
    chart?.dispose();
    chart = null;
  });
</script>
```

- [ ] **Step 4: 格式化工具**

Create `src/views/kubernetes/monitor/utils.ts`：

```ts
export const formatCpu = (millis?: number) =>
  millis == null || millis < 0 ? '—' : millis >= 1000 ? `${(millis / 1000).toFixed(1)} cores` : `${millis}m`;

export const formatMemory = (bytes?: number) => {
  if (bytes == null || bytes < 0) return '—';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let v = bytes;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v.toFixed(1)} ${units[i]}`;
};
```

- [ ] **Step 5: 验证 + Commit**

Run: `pnpm type:check`（无新增错误）

```bash
git add streampark-console/streampark-console-webapp/src/views/kubernetes
git commit -m "[K8s] Add kubernetes monitor page shell and overview tab"
```

---

### Task 11: 节点 Tab + 趋势抽屉

**Files:**
- Create: `src/views/kubernetes/monitor/tabs/NodesTab.vue`
- Create: `src/views/kubernetes/monitor/components/TrendDrawer.vue`

- [ ] **Step 1: NodesTab.vue**

要点：
- a-table 列：name、roles、status（Badge：Ready=success/其他=error）、version、internalIp、CPU（used/allocatable + 百分比进度条 formatCpu）、内存（formatMemory）、操作（详情）
- 详情 → TrendDrawer 打开（props: clusterId, kind='node', name）

- [ ] **Step 2: TrendDrawer.vue**

要点：
- props: `{ clusterId, kind, name?, namespace? }`；a-drawer + range a-radio-group（1h/6h/24h/7d）
- fetchK8sHistory → 双 echarts 折线图（CPU millis、Memory bytes，X 轴时间 HH:mm 或 MM-DD HH:mm）
- 未配置 Prometheus 判断：由父组件传入 `prometheusUrl`（index.vue 里集群列表对象），为空时 drawer 显示 prometheusMissing 文案
- 接口报错时显示错误态（ApiAlertException 消息已由全局拦截）

- [ ] **Step 3: 验证 + Commit**

Run: `pnpm type:check`

```bash
git add streampark-console/streampark-console-webapp/src/views/kubernetes
git commit -m "[K8s] Add nodes tab with history trend drawer"
```

---

### Task 12: 工作负载 Tab（Deployments + Pods）

**Files:**
- Create: `src/views/kubernetes/monitor/tabs/WorkloadsTab.vue`

- [ ] **Step 1: WorkloadsTab.vue**

要点：
- 筛选行：namespace a-select（fetchK8sNamespaces，可清空）、keyword a-input-search
- Deployments a-table：name、namespace、副本 `replicasReady/replicasDesired`（ready<desired 时红色）、image、creationTimestamp、StreamPark 标注列（streamParkAppId 非空 → a-tag color="blue" 显示 `streamParkManaged: appName`，行加浅蓝背景 rowClassName）
- Pods a-table：name、namespace、phase（Badge success/warning/error/default 映射 Running/Pending/Failed/其他）、restarts、cpuMillis(formatCpu)、memoryBytes(formatMemory)、ip、nodeName
- Pod 行操作：详情 → TrendDrawer(kind='pod', name, namespace)
- watch clusterId/reloadKey/筛选 重拉

- [ ] **Step 2: 验证 + Commit**

Run: `pnpm type:check`

```bash
git add streampark-console/streampark-console-webapp/src/views/kubernetes
git commit -m "[K8s] Add workloads tab with StreamPark Flink deployment highlighting"
```

---

### Task 13: Flink 应用 Tab

**Files:**
- Create: `src/views/kubernetes/monitor/tabs/FlinkAppsTab.vue`

- [ ] **Step 1: FlinkAppsTab.vue**

要点：
- a-table 列：appName、k8sNamespace、deployMode（tag：5=Session 6=Application）、jobState（Badge 映射 RUNNING=绿/FINISHED=蓝/CANCELED=灰/其他=红）、podPhase、restarts、cpuMillis、memoryBytes
- 空数据 Empty 提示
- watch clusterId + reloadKey 拉 fetchK8sFlinkApps

- [ ] **Step 2: 验证 + Commit**

Run: `pnpm type:check`

```bash
git add streampark-console/streampark-console-webapp/src/views/kubernetes
git commit -m "[K8s] Add flink apps tab for applications running on kubernetes"
```

---

### Task 14: 全量验证

- [ ] **Step 1: 后端全编译 + 全模块测试**

Run: `mvn -q -pl streampark-console/streampark-console-service -am compile -DskipTests && mvn -q -pl streampark-console/streampark-console-service test -Dtest='K8s*'`
Expected: BUILD SUCCESS，全部 PASS

- [ ] **Step 2: 前端构建**

Run: `cd streampark-console/streampark-console-webapp && pnpm build`
Expected: 构建成功

- [ ] **Step 3: lint**

Run: `cd streampark-console/streampark-console-webapp && npx eslint "src/views/kubernetes/**/*.{vue,ts}" "src/api/kubernetes/**/*.ts" --fix`
Expected: 无错误（--fix 自动修复格式）

- [ ] **Step 4: 数据库脚本一致性核对**

人工核对：mysql-schema / pgsql-schema / schema-h2 三份均含 t_k8s_cluster；mysql-data / pgsql-data 均含 160000 段菜单。

- [ ] **Step 5: 最终 Commit（如有 lint fix 残留）**

```bash
git add -A && git commit -m "[K8s] Final lint fixes for kubernetes monitoring feature" || true
```

---

## 验证清单（映射 Spec）

| Spec 要求 | Task |
|---|---|
| t_k8s_cluster 三份 schema | 1 |
| 多集群注册 CRUD + 测试连接 | 3 |
| fabric8 + metrics.k8s.io 实时值 + metrics-server 降级 | 2, 4 |
| 集群总览/节点/工作负载/事件 API | 4 |
| Prometheus 可选历史趋势（未配置隐藏） | 5, 11 |
| StreamPark Flink 应用标注 | 6, 12 |
| Flink 应用清单（watcher 状态 + Pod 用量） | 6, 13 |
| 菜单 + 权限码 + 角色 | 7 |
| 前端集群管理页 | 9 |
| 监控页 4 Tab + echarts + 自动刷新 | 10-13 |
| 全量构建验证 | 14 |
