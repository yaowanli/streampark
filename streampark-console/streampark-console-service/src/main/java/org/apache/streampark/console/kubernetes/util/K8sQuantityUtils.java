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

    /** fabric8 already scales the value to base units (cores), so millis = value * 1000. */
    public static long cpuToMillis(@Nullable Quantity quantity) {
        if (quantity == null || quantity.getNumericalAmount() == null) {
            return -1L;
        }
        return quantity.getNumericalAmount().movePointRight(3).longValue();
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
