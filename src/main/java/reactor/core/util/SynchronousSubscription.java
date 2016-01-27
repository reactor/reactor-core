/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.util;

import java.util.Collection;
import java.util.Iterator;

/**
 * Base class for synchronous sources which have fixed size and can
 * emit its items in a pull fashion, thus avoiding the request-accounting
 * overhead in many cases.
 *
 * @param <T> the content value type
 */
public abstract class SynchronousSubscription<T> implements FusionSubscription<T> {
	@Override
	public final boolean offer(T e) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final int size() {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final boolean contains(Object o) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final Iterator<T> iterator() {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final Object[] toArray() {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final <U> U[] toArray(U[] a) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final boolean remove(Object o) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final boolean addAll(Collection<? extends T> c) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public boolean enableOperatorFusion() {
		return true;
	}

	@Override
	public final boolean add(T e) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final T remove() {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final T element() {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}
}
