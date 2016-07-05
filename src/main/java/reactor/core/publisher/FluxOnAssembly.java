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

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedTransferQueue;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Fuseable;
import reactor.core.flow.Receiver;
import reactor.core.tuple.Tuple;
import reactor.core.tuple.Tuple2;
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
final class FluxOnAssembly<T> extends FluxSource<T, T> implements Fuseable, AssemblyOp {

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

	static Publisher<?> getParentOrThis(Publisher<?> parent) {
		Object next = parent;

		for (; ; ) {
			if (next instanceof Receiver) {
				Receiver r = (Receiver) next;
					next = r.upstream();
					if (next instanceof AssemblyOp) {
						return (Publisher<?>) next;
					}
					continue;
			}
			break;
		}
		return parent;
	}

	static String takeStacktrace(Publisher<?> source) {
		StackTraceElement[] stes = Thread.currentThread()
		                                 .getStackTrace();

		StringBuilder sb =
				new StringBuilder("\nAssembly trace from producer [" + source.getClass()
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
			source.subscribe(new OnAssemblyConditionalSubscriber<>(cs, stacktrace, this));
		}
		else {
			source.subscribe(new OnAssemblySubscriber<>(s, stacktrace, this));
		}
	}

	/**
	 * The holder for the assembly stacktrace (as its message).
	 */
	static final class OnAssemblyException extends RuntimeException {

		final Publisher<?> parent;
		final Map<Integer, Map<Integer, String>> stackByPublisher = new HashMap<>();


		/** */
		private static final long serialVersionUID = 5278398300974016773L;

		public OnAssemblyException(String message, Publisher<?> parent) {
			super(message);
			Map<Integer, String> thiz = new HashMap<>();
			thiz.put(parent.hashCode(), extract(message));
			stackByPublisher.put(0, thiz);
			this.parent = parent;
		}

		@Override
		public String getMessage() {
			StringBuilder sb = new StringBuilder(super.getMessage())
					.append("Backtraced Operator chain :\n");

			Map<Integer, String> op;
			Tuple2<Integer, Integer> next;
			Queue<Tuple2<Integer, Integer>> nexts = new LinkedTransferQueue<>();
			nexts.add(Tuple.of(0, 0));

			synchronized (stackByPublisher) {
				while ((next = nexts.poll()) != null) {
					op = stackByPublisher.get(next.getT2());
					if (op != null) {
						int i = next.getT1();
						for (Map.Entry<Integer, String> entry : op.entrySet()) {
							mapLine(i, sb, entry.getValue());
							nexts.add(Tuple.of(i, entry.getKey()));
							i++;
						}
					}
				}
			}
			return sb.toString();
		}

		void mapLine(int indent, StringBuilder sb, String s) {
			for(int i = 0; i < indent; i++){
				sb.append("\t");
			}
			sb.append("\t|_")
			  .append(s)
			  .append("\n");
		}

		@Override
		public synchronized Throwable fillInStackTrace() {
			return this;
		}

		void add(Publisher<?> parent, String stacktrace) {
			int key = getParentOrThis(parent).hashCode();
			synchronized (stackByPublisher) {
				stackByPublisher.compute(key,
						(k, s) -> {
							if (s == null) {
								s = new HashMap<>();
							}
							//only one publisher occurence possible?
							s.put(parent.hashCode(), extract(stacktrace));
							return s;
						});
			}
		}

		String extract(String source) {
			String usercode = null;
			String last = null;
			boolean first = true;
			for (String s : source.split("\n")) {
				if (s.isEmpty()) {
					continue;
				}
				if (first) {
					first = false;
					continue;
				}
				if (!s.contains("reactor.core.publisher.Mono") && !s.contains("reactor" + ".core.publisher.Flux")) {
					usercode = s.substring(s.indexOf('('));
					break;
				}
				else {
					last = s.replace("reactor.core.publisher.", "");
					last = last.substring(0, last.indexOf("("));
				}
			}

			return (last != null ? last : "") + usercode;
		}
	}

	static class OnAssemblySubscriber<T>
			implements Subscriber<T>, QueueSubscription<T> {

		final String                stacktrace;
		final Subscriber<? super T> actual;
		final Publisher<?>          parent;

		QueueSubscription<T> qs;
		Subscription         s;
		int                  fusionMode;

		OnAssemblySubscriber(Subscriber<? super T> actual,
				String stacktrace,
				Publisher<?> parent) {
			this.actual = actual;
			this.stacktrace = stacktrace;
			this.parent = parent;
		}

		@Override
		final public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		final public void onError(Throwable t) {
			fail(t);
			actual.onError(t);
		}

		@Override
		final public void onComplete() {
			actual.onComplete();
		}

		@Override
		final public int requestFusion(int requestedMode) {
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

		final void fail(Throwable t) {
			boolean set = false;
			if (t.getSuppressed().length > 0) {
				for (Throwable e : t.getSuppressed()) {
					if (e instanceof OnAssemblyException) {
						OnAssemblyException oae = ((OnAssemblyException) e);
						oae.add(parent, stacktrace);
						set = true;
						break;
					}
				}
			}
			if (!set) {
				t.addSuppressed(new OnAssemblyException(stacktrace, parent));
			}

		}

		@Override
		final public boolean isEmpty() {
			try {
				return qs.isEmpty();
			}
			catch (final Throwable ex) {
				Exceptions.throwIfFatal(ex);
				fail(ex);
				throw ex;
			}
		}

		@Override
		final public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;
				this.qs = BackpressureUtils.as(s);
				actual.onSubscribe(this);
			}
		}

		@Override
		final public int size() {
			return qs.size();
		}

		@Override
		final public void clear() {
			qs.clear();
		}

		@Override
		final public void request(long n) {
			s.request(n);
		}

		@Override
		final public void cancel() {
			s.cancel();
		}

		@Override
		final public T poll() {
			try {
				return qs.poll();
			}
			catch (final Throwable ex) {
				Exceptions.throwIfFatal(ex);
				fail(ex);
				throw ex;
			}
		}
	}

	static final class OnAssemblyConditionalSubscriber<T> extends OnAssemblySubscriber<T>
			implements ConditionalSubscriber<T> {

		final ConditionalSubscriber<? super T> actualCS;

		OnAssemblyConditionalSubscriber(ConditionalSubscriber<? super T> actual,
				String stacktrace,
				Publisher<?> parent) {
			super(actual, stacktrace, parent);
			this.actualCS = actual;
		}

		@Override
		public boolean tryOnNext(T t) {
			return actualCS.tryOnNext(t);
		}

	}
}

interface AssemblyOp {

}