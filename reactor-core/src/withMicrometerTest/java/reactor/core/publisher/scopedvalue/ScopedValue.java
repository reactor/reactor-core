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

import java.util.logging.Logger;

import static java.util.logging.Level.INFO;

// NOTE: This is a copy from the context-propagation library. Any changes should be
// considered in the upstream first. Please keep in sync.

/**
 * Serves as an abstraction of a value which can be in the current Thread-local scope.
 *
 * @author Dariusz Jędrzejczyk
 */
public interface ScopedValue {

    /**
     * Creates a new instance, which can be set in scope via {@link #open()}.
     * @param value {@code String} value associated with created {@link ScopedValue}
     * @return new instance
     */
    static ScopedValue create(String value) {
        return new SimpleScopedValue(value);
    }

    /**
     * Creates a dummy instance used for nested scopes, in which the value should be
     * virtually absent, but allows reverting to the previous value in scope.
     * @return new instance representing an empty scope
     */
    static ScopedValue nullValue() {
        return new NullScopedValue();
    }

    /**
     * {@code String} value associated with this instance.
     * @return associated value
     */
    String get();

    /**
     * Create a new scope and set the value for this Thread.
     * @return newly created {@link Scope}
     */
    Scope open();

    /**
     * Represents a scope in which a {@link ScopedValue} is set for a particular Thread
     * and maintains a hierarchy between this instance and the parent.
     */
    class Scope implements AutoCloseable {

        private static final Logger log = Logger.getLogger(Scope.class.getName());

        final ScopedValue scopedValue;

        final Scope parentScope;

        Scope(ScopedValue scopedValue) {
            log.log(INFO, () -> String.format("%s: open scope[%s]", scopedValue.get(), hashCode()));
            this.scopedValue = scopedValue;

            Scope currentScope = ScopeHolder.get();
            this.parentScope = currentScope;
            ScopeHolder.set(this);
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

}
