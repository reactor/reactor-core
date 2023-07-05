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

package reactor.core.publisher.scopedvalue;

import io.micrometer.context.ThreadLocalAccessor;

// NOTE: This is a copy from the context-propagation library. Any changes should be
// considered in the upstream first. Please keep in sync.

/**
 * Accessor for {@link ScopedValue}.
 *
 * @author Dariusz Jędrzejczyk
 */
public class ScopedValueThreadLocalAccessor implements ThreadLocalAccessor<ScopedValue> {

    /**
     * The key used for registrations in {@link io.micrometer.context.ContextRegistry}.
     */
    public static final String KEY = "svtla";

    @Override
    public Object key() {
        return KEY;
    }

    @Override
    public ScopedValue getValue() {
        return ScopeHolder.currentValue();
    }

    @Override
    public void setValue(ScopedValue value) {
        value.open();
    }

    @Override
    public void setValue() {
        ScopedValue.nullValue().open();
    }

    @Override
    public void restore(ScopedValue previousValue) {
        ScopedValue.Scope currentScope = ScopeHolder.get();
        if (currentScope != null) {
            if (currentScope.parentScope == null || currentScope.parentScope.scopedValue != previousValue) {
                throw new RuntimeException("Restoring to a different previous scope than expected!");
            }
            currentScope.close();
        }
        else {
            throw new RuntimeException("Restoring to previous scope, but current is missing.");
        }
    }

    @Override
    public void restore() {
        ScopedValue.Scope currentScope = ScopeHolder.get();
        if (currentScope != null) {
            currentScope.close();
        }
        else {
            throw new RuntimeException("Restoring to previous scope, but current is missing.");
        }
    }

}
