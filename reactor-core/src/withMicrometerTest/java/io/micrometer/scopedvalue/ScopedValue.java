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

import java.util.Objects;

// NOTE: This is a copy from the context-propagation library. Any changes should be
// considered in the upstream first. Please keep in sync.

/**
 * Serves as an abstraction of a value which can be in the current Thread-local
 * {@link Scope scope} that maintains the hierarchy between the parent a new scope with
 * potentially a different value.
 *
 * @author Dariusz Jędrzejczyk
 */
public class ScopedValue {

    private final String value;

    private ScopedValue(String value) {
        this.value = value;
    }

    /**
     * Creates a new instance, which can be set in scope via {@link Scope#open()}.
     * @param value {@code String} value associated with created {@link ScopedValue}
     * @return new instance
     */
    public static ScopedValue create(String value) {
        Objects.requireNonNull(value, "value can't be null");
        return new ScopedValue(value);
    }

    /**
     * Creates a dummy instance used for nested scopes, in which the value should be
     * virtually absent, but allows reverting to the previous value in scope.
     * @return new instance representing an empty scope
     */
    public static ScopedValue nullValue() {
        return new ScopedValue(null);
    }

    /**
     * {@code String} value associated with this instance.
     * @return associated value
     */
    public String get() {
        return value;
    }

}
