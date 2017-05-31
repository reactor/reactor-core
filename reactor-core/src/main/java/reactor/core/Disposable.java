/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
 */
@FunctionalInterface
public interface Disposable {

	/**
	 * Cancel or dispose the underlying task or resource.
	 * <p>
	 * Implementations are required to make this method idempotent.
	 */
	void dispose();

	/**
	 * Optionally return {@literal true} when the resource or task is disposed.
	 * <p>
	 * Implementations are not required to track disposition and as such may never
	 * return {@literal true} even when disposed. However, they MUST only return true
	 * when there's a guarantee the resource or task is disposed.
	 *
	 * @return {@literal true} when there's a guarantee the resource or task is disposed.
	 */
	default boolean isDisposed() {
		return false;
	}
}
