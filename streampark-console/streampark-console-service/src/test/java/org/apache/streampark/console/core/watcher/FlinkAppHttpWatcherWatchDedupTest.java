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

package org.apache.streampark.console.core.watcher;

import org.apache.streampark.console.core.entity.FlinkApplication;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies the in-flight dedup of {@link FlinkAppHttpWatcher#watch}. */
class FlinkAppHttpWatcherWatchDedupTest {

    private final List<Runnable> queuedTasks = new ArrayList<>();
    private final Executor manualExecutor = queuedTasks::add;

    private FlinkAppHttpWatcher watcher;
    private Method watch;
    private Field inProgress;

    @BeforeEach
    void setUp() throws Exception {
        watcher = new FlinkAppHttpWatcher();
        Field executorField = FlinkAppHttpWatcher.class.getDeclaredField("watchExecutor");
        executorField.setAccessible(true);
        executorField.set(watcher, manualExecutor);

        watch = FlinkAppHttpWatcher.class.getDeclaredMethod("watch", Long.class, FlinkApplication.class);
        watch.setAccessible(true);

        inProgress = FlinkAppHttpWatcher.class.getDeclaredField("WATCHING_IN_PROGRESS");
        inProgress.setAccessible(true);
        ((Set<?>) inProgress.get(null)).clear();

        queuedTasks.clear();
    }

    @Test
    void skipsDuplicateWatchWhileTaskInFlight() throws Exception {
        invokeWatch(1L);
        assertThat(queuedTasks).hasSize(1);

        invokeWatch(1L);
        assertThat(queuedTasks).hasSize(1);

        invokeWatch(2L);
        assertThat(queuedTasks).hasSize(2);
    }

    @SuppressWarnings("unchecked")
    private Set<Long> inProgressClaims() throws Exception {
        return (Set<Long>) inProgress.get(null);
    }

    @Test
    void releasesClaimAfterTaskRuns() throws Exception {
        invokeWatch(1L);
        assertThat(queuedTasks).hasSize(1);

        // the runnable may hit unset dependencies on a bare instance; the claim release
        // under test happens in its finally block regardless of the outcome
        try {
            queuedTasks.get(0).run();
        } catch (Throwable expected) {
            // ignore
        }

        assertThat(inProgressClaims()).doesNotContain(1L);

        invokeWatch(1L);
        assertThat(queuedTasks).hasSize(2);
    }

    @Test
    void releasesClaimWhenQueueRejectsSubmission() throws Exception {
        Field executorField = FlinkAppHttpWatcher.class.getDeclaredField("watchExecutor");
        executorField.setAccessible(true);
        Executor rejectingExecutor = task -> {
            throw new RejectedExecutionException("queue full");
        };
        executorField.set(watcher, rejectingExecutor);

        assertThatThrownBy(() -> invokeWatch(1L))
            .isInstanceOf(RejectedExecutionException.class);
        assertThat(inProgressClaims()).doesNotContain(1L);

        executorField.set(watcher, manualExecutor);
        invokeWatch(1L);
        assertThat(queuedTasks).hasSize(1);
    }

    private void invokeWatch(Long appId) throws Exception {
        try {
            watch.invoke(watcher, appId, new FlinkApplication());
        } catch (InvocationTargetException e) {
            throw (Exception) e.getCause();
        }
    }
}
