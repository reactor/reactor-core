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

// NOTE: This is a copy from the context-propagation library. Any changes should be
// considered in the upstream first. Please keep in sync.

/**
 * Implementation of {@link ScopedValue} for which {@link ScopedValue.Scope} maintains the
 * hierarchy between parent scope and an opened scope for this value.
 */
class SimpleScopedValue implements ScopedValue {

    private final String value;

    SimpleScopedValue(String value) {
        this.value = value;
    }

    @Override
    public String get() {
        return value;
    }

    @Override
    public Scope open() {
        return new Scope(this);
    }

}
