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

import java.util.logging.Logger;

import static java.util.logging.Level.INFO;

// NOTE: This is a copy from the context-propagation library. Any changes should be
// considered in the upstream first. Please keep in sync.

/**
 * Represents a scope in which a {@link ScopedValue} is set for a particular Thread and
 * maintains a hierarchy between this instance and the parent.
 */
public class Scope implements AutoCloseable {

    private static final Logger log = Logger.getLogger(Scope.class.getName());

    final ScopedValue scopedValue;

    final Scope parentScope;

    private Scope(ScopedValue scopedValue, Scope parentScope) {
        log.log(INFO, () -> String.format("%s: open scope[%s]", scopedValue.get(), hashCode()));
        this.scopedValue = scopedValue;
        this.parentScope = parentScope;
    }

    /**
     * Create a new scope and set the value for this Thread.
     * @return newly created {@link Scope}
     */
    public static Scope open(ScopedValue value) {
        Scope scope = new Scope(value, ScopeHolder.get());
        ScopeHolder.set(scope);
        return scope;
    }

    @Override
    public void close() {
        if (parentScope == null) {
            log.log(INFO, () -> String.format("%s: remove scope[%s]", scopedValue.get(), hashCode()));
            ScopeHolder.remove();
        }
        else {
            log.log(INFO, () -> String.format("%s: close scope[%s] -> restore %s scope[%s]", scopedValue.get(),
                    hashCode(), parentScope.scopedValue.get(), parentScope.hashCode()));
            ScopeHolder.set(parentScope);
        }
    }

}
