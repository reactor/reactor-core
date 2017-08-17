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

package reactor.core.scheduler;

import java.util.Collection;

import reactor.core.Disposable;
import reactor.core.Disposable.CompositeDisposable;

/**
 * A generic {@link CompositeDisposable} that is empty and no-op.
 *
 * @author Simon Baslé
 */
final class EmptyDisposableContainer<T extends Disposable> implements CompositeDisposable<T> {

	@Override
	public boolean add(T d) {
		return false;
	}

	@Override
	public boolean addAll(Collection<T> ds) {
		return false;
	}

	@Override
	public boolean remove(T d) {
		return false;
	}

	@Override
	public boolean removeAndDispose(T disposable) {
		return false;
	}

	@Override
	public void clear() {

	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public void dispose() {

	}

	@Override
	public boolean isDisposed() {
		return false;
	}
}
