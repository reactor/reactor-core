/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core;

/**
 * Indicates that a task or resource can be cancelled/disposed.
 * <p>Call to the dispose method is/should be idempotent.
 *
 * @deprecated use {@link Disposable}, will be removed in 3.1.0
 */
@FunctionalInterface
@Deprecated
public interface Cancellation {
    /**
     * Cancel or dispose the underlying task or resource.
     * <p>Call to this method is/should be idempotent.
     */
    void dispose();
}
