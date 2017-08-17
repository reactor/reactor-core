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

/**
 * A generic {@link Composite} that is empty and no-op.
 *
 * @author Simon Baslé
 */
final class EmptyCompositeDisposable implements Disposable.Composite {

	@Override
	public boolean add(Disposable d) {
		return false;
	}

	@Override
	public boolean addAll(Collection<? extends Disposable> ds) {
		return false;
	}

	@Override
	public boolean remove(Disposable d) {
		return false;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public void dispose() {	}

	@Override
	public boolean isDisposed() {
		return false;
	}

	static final Disposable.Composite DISPOSED_PARENT = new EmptyCompositeDisposable();
	static final Disposable.Composite DONE_PARENT = new EmptyCompositeDisposable();
}
