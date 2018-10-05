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
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;

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

	static boolean isContextUnsupportedFatal;

	static {
		reload();
	}

	static void reload() {
		isContextUnsupportedFatal = Boolean.parseBoolean(System.getProperty(CONTEXT_UNSUPPORTED_PROPERTY, "false")); //TODO 3.3 make fatal by default
	}

	private static final Logger LOGGER = Loggers.getLogger(ContextUnsupported.class);

	private static final String ERROR_GET = "Context#get(Object) is not supported due to Context-incompatible element in the chain";
	private static final String ERROR_GETCLASS = "Context#get(Class) is not supported due to Context-incompatible element in the chain";
	private static final String ERROR_GETORDEFAULT = "Context#getOrDefault is not supported due to Context-incompatible element in the chain";
	private static final String ERROR_GETOREMPTY = "Context#getOrEmpty is not supported due to Context-incompatible element in the chain";
	private static final String ERROR_STREAM = "Context#stream is not supported due to Context-incompatible element in the chain";

	@NonNull
	final String           reason;
	final RuntimeException tracebackException;

	ContextUnsupported(@NonNull String reason, boolean fatal) {
		this.reason = Objects.requireNonNull(reason, "reason required");
		if (fatal) {
			this.tracebackException = new RuntimeException(reason);
		}
		else {
			this.tracebackException = null;
		}
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
		if (tracebackException != null) {
			throw new UnsupportedOperationException(ERROR_GET, tracebackException);
		}
		else {
			LOGGER.error("{}: {}", ERROR_GET, reason);
			throw new NoSuchElementException("Context is empty");
		}
	}

	@Override
	public <T> T get(Class<T> key) {
		if (tracebackException != null) {
			throw new UnsupportedOperationException(ERROR_GETCLASS, tracebackException);
		}
		else {
			LOGGER.error("{}: {}", ERROR_GETCLASS, reason);
			throw new NoSuchElementException("Context does not contain a value of type "+key.getName());
		}
	}

	@Nullable@Override
	public <T> T getOrDefault(Object key, @Nullable T defaultValue) {
		if (tracebackException != null) {
			throw new UnsupportedOperationException(ERROR_GETORDEFAULT, tracebackException);
		}
		else {
			LOGGER.error("{}: {}", ERROR_GETORDEFAULT, reason);
			return defaultValue;
		}
	}

	@Override
	public <T> Optional<T> getOrEmpty(Object key) {
		if (tracebackException != null) {
			throw new UnsupportedOperationException(ERROR_GETOREMPTY, tracebackException);
		}
		else {
			LOGGER.error("{}: {}", ERROR_GETOREMPTY, reason);
			return Optional.empty();
		}
	}

	@Override
	public Stream<Map.Entry<Object, Object>> stream() {
		if (tracebackException != null) {
			throw new UnsupportedOperationException(ERROR_STREAM, tracebackException);
		}
		else {
			LOGGER.error("{}: {}", ERROR_STREAM, reason);
			return Stream.empty();
		}
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
