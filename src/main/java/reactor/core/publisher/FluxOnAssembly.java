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

import java.util.LinkedList;
import java.util.List;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Receiver;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

/**
 * Captures the current stacktrace when this publisher is created and
 * makes it available/visible for debugging purposes from
 * the inner Subscriber.
 * <p>
 * Note that getting a stacktrace is a costly operation.
 * <p>
 * The operator sanitizes the stacktrace and removes noisy entries such as:
 * <ul>
 * <li>java.lang.Thread entries</li>
 * <li>method references with source line of 1 (bridge methods)</li>
 * <li>Tomcat worker thread entries</li>
 * <li>JUnit setup</li>
 * </ul>
 *
 * @param <T> the value type passing through
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 */
final class FluxOnAssembly<T> extends FluxSource<T, T> implements Fuseable, AssemblyOp {

	final AssemblySnapshotException snapshotStack;

	/**
	 * If set to true, the creation of FluxOnAssembly will capture the raw stacktrace
	 * instead of the sanitized version.
	 */
	static final boolean fullStackTrace = Boolean.parseBoolean(System.getProperty(
			"reactor.trace.assembly.fullstacktrace",
			"false"));

	static final String CHECKPOINT_LOGGER_NAME = "reactor.checkpoint";

	static final Logger CHECKPOINT_LOGGER = Loggers.getLogger(CHECKPOINT_LOGGER_NAME);

	/**
	 * Create an assembly trace decorated as a {@link Flux}.
	 */
	FluxOnAssembly(Publisher<? extends T> source) {
		super(source);
		this.snapshotStack = new AssemblySnapshotException();
	}

	/**
	 * Create an assembly trace augmented with a custom description (eg. a name for a Flux
	 * or a wider correlation ID) and exposed as a {@link Flux}.
	 */
	FluxOnAssembly(Publisher<? extends T> source, String description) {
		super(source);
		this.snapshotStack = new AssemblySnapshotException(description);
	}

	static String getStacktrace(Publisher<?> source, AssemblySnapshotException snapshotStack) {
		StackTraceElement[] stes = snapshotStack.getStackTrace();

		StringBuilder sb = new StringBuilder();
		if (null != source) {
			fillStacktraceHeader(sb, source.getClass(), snapshotStack);
		}

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
				if (row.contains("reactor.core.publisher.ParallelFlux.onAssembly")) {
					continue;
				}
				if (row.contains("reactor.core.publisher.SignalLogger")) {
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
				if (row.contains("FluxCallableOnAssembly.")) {
					continue;
				}
				if (row.contains("OnOperatorCreate")) {
					continue;
				}
				if (row.contains("operatorStacktrace")) {
					continue;
				}
				if (row.contains("reactor.core.publisher.Hooks")) {
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

	static void fillStacktraceHeader(StringBuilder sb, Class<?> sourceClass,
			AssemblySnapshotException ase) {
		sb.append("\nAssembly trace from producer [")
		  .append(sourceClass.getName())
		  .append("]");

		if (ase.getMessage() != null) {
			sb.append(", described as [")
			  .append(ase.getMessage())
			  .append("]");
		}

		sb.append(" :\n");
	}

	@SuppressWarnings("unchecked")
	static <T> void subscribe(Subscriber<? super T> s, Publisher<? extends T> source,
			AssemblySnapshotException snapshotStack) {

		if(snapshotStack != null) {
			if (s instanceof ConditionalSubscriber) {
				ConditionalSubscriber<? super T> cs = (ConditionalSubscriber<? super T>) s;
				source.subscribe(new OnAssemblyConditionalSubscriber<>(cs,
						snapshotStack, source));
			}
			else {
				source.subscribe(new OnAssemblySubscriber<>(s, snapshotStack, source));
			}
		}
	}

	static String extract(String source, boolean skipFirst) {
		String usercode = null;
		String last = null;
		boolean first = skipFirst;
		for (String s : source.split("\n")) {
			if (s.isEmpty()) {
				continue;
			}
			if (first) {
				first = false;
				continue;
			}
			if (!s.contains("reactor.core.publisher")) {
				usercode = s.substring(s.indexOf('('));
				break;
			}
			else if(last == null){
				last = s.replace("reactor.core.publisher.", "");
				last = last.substring(0, last.indexOf("("));
			}
		}

		return (skipFirst && last != null ? last : "") + usercode;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		subscribe(s, source, snapshotStack);
	}

	/**
	 * The exception that captures assembly context, possibly with a user-readable
	 * description or a wider correlation ID (which serves as the exception's
	 * {@link #getMessage() message}). Use the empty constructor if the later is not
	 * relevant.
	 */
	static final class AssemblySnapshotException extends RuntimeException {

		AssemblySnapshotException() {
			super();
		}

		/**
		 * @param description a description for the assembly traceback.
		 * Use {@link #AssemblySnapshotException()} rather than null if not relevant.
		 */
		AssemblySnapshotException(String description) {
			super(description);
		}
	}

	/**
	 * The holder for the assembly stacktrace (as its message).
	 */
	static final class OnAssemblyException extends RuntimeException {

		final List<Tuple3<Integer, String, Integer>> chainOrder = new LinkedList<>();

		/** */
		private static final long serialVersionUID = 5278398300974016773L;

		OnAssemblyException(String message, Publisher<?> parent) {
			super(message);
			chainOrder.add(Tuples.of(parent.hashCode(), extract(message, true), 0));
		}

		void mapLine(int indent, StringBuilder sb, String s) {
			for (int i = 0; i < indent; i++) {
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
			synchronized (chainOrder) {
				int i = 0;

				int n = chainOrder.size();
				int j = n - 1;
				Tuple3<Integer, String, Integer> tmp;
				while(j >= 0){
					tmp = chainOrder.get(j);
					if(tmp.getT1() == key){
						i = tmp.getT3();
						break;
					}
					j--;
				}


				for(;;){
					Tuple3<Integer, String, Integer> t =
							Tuples.of(
									parent.hashCode(),
									extract(stacktrace, true), i);

					if(!chainOrder.contains(t)){
						chainOrder.add(t);
						break;
					}
					i++;
				}
			}
		}

		@Override
		public String getMessage() {
			StringBuilder sb = new StringBuilder(super.getMessage()).append(
					"Error has been observed by the following operators, starting from the origin :\n");

			synchronized (chainOrder) {
				for(Tuple3<Integer, String, Integer> t : chainOrder) {
					mapLine(t.getT3(), sb, t.getT2());
				}
			}
			return sb.toString();
		}
	}

	static Publisher<?> getParentOrThis(Publisher<?> parent) {
		Object next = parent;

		for (; ; ) {
			if (next instanceof Receiver) {
				Receiver r = (Receiver) next;
				next = r.upstream();
				if (next instanceof AssemblyOp) {
					continue;
				}
				return (Publisher<?>) next;
			}
			break;
		}
		return parent;
	}

	static class OnAssemblySubscriber<T> implements Subscriber<T>, QueueSubscription<T> {

		final AssemblySnapshotException snapshotStack;
		final Subscriber<? super T>     actual;
		final Publisher<?>              parent;

		QueueSubscription<T> qs;
		Subscription         s;
		int                  fusionMode;

		OnAssemblySubscriber(Subscriber<? super T> actual,
				AssemblySnapshotException snapshotStack,
				Publisher<?> parent) {
			this.actual = actual;
			this.snapshotStack = snapshotStack;
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
						oae.add(parent, getStacktrace(parent, snapshotStack));
						set = true;
						break;
					}
				}
			}
			if (!set) {
				t.addSuppressed(new OnAssemblyException(getStacktrace(parent,
						snapshotStack), parent));
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
			if (Operators.validate(this.s, s)) {
				this.s = s;
				this.qs = Operators.as(s);
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
				AssemblySnapshotException stacktrace,
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