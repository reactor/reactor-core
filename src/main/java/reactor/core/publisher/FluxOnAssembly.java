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

package reactor.core.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Fuseable;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.Exceptions;

/**
 * Captures the current stacktrace when this publisher is created and makes it
 * available/visible for debugging purposes from the inner Subscriber.
 * <p>
 * Note that getting a stacktrace is a costly operation.
 * <p>
 * The operator sanitizes the stacktrace and removes noisy entries such as: <ul>
 * <li>java.lang.Thread entries</li> <li>method references with source line of 1 (bridge
 * methods)</li> <li>Tomcat worker thread entries</li> <li>JUnit setup</li> </ul>
 *
 * @param <T> the value type passing through
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 * @since 2.5
 */
final class FluxOnAssembly<T> extends FluxSource<T, T> implements Fuseable {

	final String stacktrace;

	/**
	 * If set to true, the creation of FluxOnAssembly will capture the raw stacktrace
	 * instead of the sanitized version.
	 */
	static final boolean fullStackTrace = Boolean.parseBoolean(System.getProperty(
			"reactor.trace.assembly.fullstacktrace",
			"false"));

	public FluxOnAssembly(Publisher<? extends T> source) {
		super(source);
		this.stacktrace = takeStacktrace(source);
	}

	static String takeStacktrace(Publisher<?> source) {
		StackTraceElement[] stes = Thread.currentThread()
		                                 .getStackTrace();

		StringBuilder sb =
				new StringBuilder("\nAssembly trace from source [" + source.getClass()
				                                                           .getName() + "] " +
						":\n");

		for (StackTraceElement e : stes) {
			String row = e.toString();
			if (!fullStackTrace) {
				if (e.getLineNumber() <= 1) {
					continue;
				}
				if (row.contains("reactor.core.publisher.Flux.onAssembly")) {
					continue;
				}
				if (row.contains("reactor.core.publisher.Mono.onAssembly")) {
					continue;
				}
				if (row.contains("FluxOnAssembly.")) {
					continue;
				}
				if (row.contains("MonoOnAssembly.")) {
					continue;
				}
				if (row.contains("MonoCallableOnAssembly.")) {
					continue;
				}
				if (row.contains(".junit.runner")) {
					continue;
				}
				if (row.contains(".junit4.runner")) {
					continue;
				}
				if (row.contains(".junit.internal")) {
					continue;
				}
				if (row.contains("sun.reflect")) {
					continue;
				}
				if (row.contains("useTraceAssembly")) {
					continue;
				}
				if (row.contains("java.lang.Thread.")) {
					continue;
				}
				if (row.contains("ThreadPoolExecutor")) {
					continue;
				}
				if (row.contains("org.apache.catalina.")) {
					continue;
				}
				if (row.contains("org.apache.tomcat.")) {
					continue;
				}
				if (row.contains("com.intellij.")) {
					continue;
				}
				if (row.contains("java.lang.reflect")) {
					continue;
				}
			}
			sb.append("\t")
			  .append(row)
			  .append("\n");
		}

		return sb.toString();
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		if (s instanceof ConditionalSubscriber) {
			ConditionalSubscriber<? super T> cs = (ConditionalSubscriber<? super T>) s;
			source.subscribe(new OnAssemblyConditionalSubscriber<>(cs, stacktrace));
		}
		else {
			source.subscribe(new OnAssemblySubscriber<>(s, stacktrace));
		}
	}

	@Override
	public boolean isTraceAssembly() {
		return true;
	}

	/**
	 * The holder for the assembly stacktrace (as its message).
	 */
	static final class OnAssemblyException extends RuntimeException {

		/** */
		private static final long serialVersionUID = 5278398300974016773L;

		public OnAssemblyException(String message) {
			super(message);
		}

		@Override
		public synchronized Throwable fillInStackTrace() {
			return this;
		}
	}

	static final class OnAssemblySubscriber<T>
			implements Subscriber<T>, QueueSubscription<T> {

		final String                stacktrace;
		final Subscriber<? super T> actual;

		QueueSubscription<T> qs;
		Subscription         s;
		int                  fusionMode;

		OnAssemblySubscriber(Subscriber<? super T> actual, String stacktrace) {
			this.actual = actual;
			this.stacktrace = stacktrace;
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (t.getSuppressed().length == 0 || !(t.getSuppressed()[0] instanceof OnAssemblyException)) {
				t.addSuppressed(new OnAssemblyException(stacktrace));
			}
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}

		@Override
		public int requestFusion(int requestedMode) {
			QueueSubscription<T> qs = this.qs;
			if (qs != null) {
				int m = qs.requestFusion(requestedMode);
				if (m != Fuseable.NONE) {
					fusionMode = m;
				}
				return m;
			}
			return Fuseable.NONE;
		}

		@Override
		public boolean isEmpty() {
			try {
				return qs.isEmpty();
			}
			catch (final Throwable ex) {
				Exceptions.throwIfFatal(ex);
				ex.addSuppressed(new OnAssemblyException(stacktrace));
				throw ex;
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;
				this.qs = BackpressureUtils.as(s);
				actual.onSubscribe(this);
			}
		}

		@Override
		public int size() {
			return qs.size();
		}

		@Override
		public void clear() {
			qs.clear();
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		public T poll() {
			try {
				return qs.poll();
			}
			catch (final Throwable ex) {
				Exceptions.throwIfFatal(ex);
				ex.addSuppressed(new OnAssemblyException(stacktrace));
				throw ex;
			}
		}
	}

	static final class OnAssemblyConditionalSubscriber<T>
			implements ConditionalSubscriber<T>, QueueSubscription<T> {

		final String                           stacktrace;
		final ConditionalSubscriber<? super T> actual;

		QueueSubscription<T> qs;
		Subscription         s;
		int                  fusionMode;

		public OnAssemblyConditionalSubscriber(ConditionalSubscriber<? super T> actual,
				String stacktrace) {
			this.actual = actual;
			this.stacktrace = stacktrace;
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public boolean tryOnNext(T t) {
			return actual.tryOnNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (t.getSuppressed().length == 0 || !(t.getSuppressed()[0] instanceof OnAssemblyException)) {
				t.addSuppressed(new OnAssemblyException(stacktrace));
			}
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}

		@Override
		public int requestFusion(int requestedMode) {
			QueueSubscription<T> qs = this.qs;
			if (qs != null) {
				int m = qs.requestFusion(requestedMode);
				if (m != Fuseable.NONE) {
					fusionMode = m;
				}
				return m;
			}
			return Fuseable.NONE;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;
				this.qs = BackpressureUtils.as(s);
				actual.onSubscribe(this);
			}
		}

		@Override
		public int size() {
			return qs.size();
		}

		@Override
		public void clear() {
			qs.clear();
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		public boolean isEmpty() {
			try {
				return qs.isEmpty();
			}
			catch (final Throwable ex) {
				Exceptions.throwIfFatal(ex);
				ex.addSuppressed(new OnAssemblyException(stacktrace));
				throw ex;
			}
		}

		@Override
		public T poll() {
			try {
				return qs.poll();
			}
			catch (final Throwable ex) {
				Exceptions.throwIfFatal(ex);
				ex.addSuppressed(new OnAssemblyException(stacktrace));
				throw ex;
			}
		}
	}
}
