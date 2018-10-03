/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.util.context;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import reactor.util.annotation.NonNull;

/**
 * A {@link Context} that rejects ALL get operations with an error that traces back
 * to a {@link org.reactivestreams.Subscriber}, usually one that adapts to-from classes
 * that are not compatible with the propagation of {@link Context} (eg. an RxJava2
 * Flowable, a plain Reactive Streams {@link org.reactivestreams.Subscriber} or a java 8 Flow.Subscriber).
 * <p>
 * The goal is to fail fast for libraries that depend on the {@link Context} propagation,
 * when such an incompatible actor is part of the chain.
 *
 * @author Simon Baslé
 */
final class ContextUnsupported implements Context {

	@NonNull
	final RuntimeException traceback;

	ContextUnsupported(@NonNull RuntimeException traceback) {
		this.traceback = Objects.requireNonNull(traceback, "traceback required");
	}

	@Override
	public boolean hasKey(Object key) {
		return false;
	}

	@Override
	public boolean isEmpty() {
		return true;
	}

	@Override
	public <T> T get(Object key) {
		throw new UnsupportedOperationException("Context#get(Object) is not supported due to Context-incompatible element in the chain",
				traceback);
	}

	@Override
	public <T> T get(Class<T> key) {
		throw new UnsupportedOperationException("Context#get(Class) is not supported due to Context-incompatible element in the chain",
				traceback);
	}

	@Override
	public <T> T getOrDefault(Object key, T defaultValue) {
		throw new UnsupportedOperationException("Context#getOrDefault is not supported due to Context-incompatible element in the chain",
				traceback);
	}

	@Override
	public <T> Optional<T> getOrEmpty(Object key) {
		throw new UnsupportedOperationException("Context#getOrEmpty is not supported due to Context-incompatible element in the chain",
				traceback);
	}

	@Override
	public Stream<Map.Entry<Object, Object>> stream() {
		throw new UnsupportedOperationException("Context#stream is not supported due to Context-incompatible element in the chain",
				traceback);
	}

	@Override
	public Context put(Object key, Object value) {
		return new Context1(key, value);
	}

	@Override
	public Context delete(Object key) {
		return this;
	}

	@Override
	public String toString() {
		return "ContextUnsupported{}";
	}
}
