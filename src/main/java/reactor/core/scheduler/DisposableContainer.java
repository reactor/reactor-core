/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.scheduler;

import reactor.core.Disposable;

/**
 * A container for {@link Disposable Disposables}, which can be added to the container
 * or removed from it (including with the extra step of calling {@link Disposable#dispose() dispose()}
 * on the removed disposable).
 *
 * @author Simon Baslé
 */
interface DisposableContainer<T extends Disposable> {

	/**
	 * Add a {@link Disposable} to this container.
	 *
	 * @param disposable the {@link Disposable} to add.
	 * @return true if the disposable could be added, false otherwise.
	 */
	boolean add(T disposable);

	/**
	 * Remove the {@link Disposable} from this container, without disposing it.
	 *
	 * @param disposable the {@link Disposable} to remove.
	 * @return true if the disposable was successfully removed, false otherwise.
	 */
	boolean remove(T disposable);

	/**
	 * Remove the {@link Disposable} from this container, and additionally call
	 * {@link Disposable#dispose() dispose()} on it (provided the removal did succeed).
	 *
	 * @param disposable the {@link Disposable} to remove and dispose.
	 * @return true if the disposable was successfully removed and disposed, false otherwise.
	 */
	default boolean removeAndDispose(T disposable) {
		if (remove(disposable)) {
			disposable.dispose();
			return true;
		}
		return false;
	}

}
