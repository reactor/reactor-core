/*
 * Copyright (c) 2015-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.util.context;

import java.util.AbstractMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Stream;

final class Context1 implements CoreContext {

	final Object key;
	final Object value;

	Context1(Object key, Object value) {
		this.key = Objects.requireNonNull(key, "key");
		this.value = Objects.requireNonNull(value, "value");
	}

	@Override
	public Context put(Object key, Object value) {
		Objects.requireNonNull(key, "key");
		Objects.requireNonNull(value, "value");

		if(this.key.equals(key)){
			return new Context1(key, value);
		}

		return new Context2(this.key, this.value, key, value);
	}

	@Override
	public Context delete(Object key) {
		Objects.requireNonNull(key, "key");
		if (this.key.equals(key)) {
			return Context.empty();
		}
		return this;
	}

	@Override
	public boolean hasKey(Object key) {
		return this.key.equals(key);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T get(Object key) {
		if (hasKey(key)) {
			return (T)this.value;
		}
		throw new NoSuchElementException("Context does not contain key: " + key);
	}

	@Override
	public Stream<Map.Entry<Object, Object>> stream() {
		return Stream.of(new AbstractMap.SimpleImmutableEntry<>(key, value));
	}

	@Override
	public Context putAllInto(Context base) {
		return base.put(key, value);
	}

	@Override
	public void unsafePutAllInto(ContextN other) {
		other.accept(key, value);
	}

	@Override
	public int size() {
		return 1;
	}

	@Override
	public String toString() {
		return "Context1{" + key + '='+ value + '}';
	}
}
