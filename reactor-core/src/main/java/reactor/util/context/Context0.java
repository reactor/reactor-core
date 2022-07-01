/*
 * Copyright (c) 2015-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

final class Context0 implements CoreContext {

	static final Context0 INSTANCE = new Context0();

	@Override
	public Context put(Object key, Object value) {
		Objects.requireNonNull(key, "key");
		Objects.requireNonNull(value, "value");
		return new Context1(key, value);
	}

	@Override
	public Context delete(Object key) {
		return this;
	}

	@Override
	public <T> T get(Object key) {
		throw new NoSuchElementException("Context is empty");
	}

	@Override
	public boolean hasKey(Object key) {
		return false;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public boolean isEmpty() {
		return true;
	}

	@Override
	public String toString() {
		return "Context0{}";
	}

	@Override
	public Stream<Map.Entry<Object, Object>> stream() {
		return Stream.empty();
	}

	@Override
	public void forEach(BiConsumer<Object, Object> action) {
	}

	@Override
	public Context putAllInto(Context base) {
		return base;
	}

	@Override
	public void unsafePutAllInto(ContextN other) {
	}
}
