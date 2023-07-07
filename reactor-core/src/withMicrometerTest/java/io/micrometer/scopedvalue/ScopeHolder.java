/*
 * Copyright (c) 2023 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.micrometer.scopedvalue;

import org.assertj.core.util.VisibleForTesting;

// NOTE: This is a copy from the context-propagation library. Any changes should be
// considered in the upstream first. Please keep in sync.

/**
 * Thread-local storage for the current value in scope for the current Thread.
 */
public class ScopeHolder {

    private static final ThreadLocal<Scope> SCOPE = new ThreadLocal<>();

    public static ScopedValue currentValue() {
        Scope scope = SCOPE.get();
        return scope == null ? null : scope.scopedValue;
    }

    public static Scope get() {
        return SCOPE.get();
    }

    static void set(Scope scope) {
        SCOPE.set(scope);
    }

    @VisibleForTesting
    public static void remove() {
        SCOPE.remove();
    }

}
