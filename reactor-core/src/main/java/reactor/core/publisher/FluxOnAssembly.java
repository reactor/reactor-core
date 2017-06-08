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
package reactor.core.publisher;

import java.util.LinkedList;
import java.util.List;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.context.Context;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;
import javax.annotation.Nullable;

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
final class FluxOnAssembly<T> extends FluxOperator<T, T> implements Fuseable, AssemblyOp {

	final AssemblySnapshotException snapshotStack;

	/**
	 * If set to true, the creation of FluxOnAssembly will capture the raw stacktrace
	 * instead of the sanitized version.
	 */
	static final boolean fullStackTrace = Boolean.parseBoolean(System.getProperty(
			"reactor.trace.assembly.fullstacktrace",
			"false"));

	/**
	 * Create an assembly trace decorated as a {@link Flux}.
	 */
	FluxOnAssembly(Flux<? extends T> source) {
		super(source);
		this.snapshotStack = new AssemblySnapshotException();
	}

	/**
	 * Create an assembly trace augmented with a custom description (eg. a name for a Flux
	 * or a wider correlation ID) and exposed as a {@link Flux}.
	 */
	FluxOnAssembly(Flux<? extends T> source, @Nullable String description) {
		this(source, description, false);
	}

	/**
	 * If light, create an assembly marker that has no trace but just shows a custom
	 * description (eg. a name for a Flux or a wider correlation ID) and exposed as a
	 * {@link Flux}. If light == false, behaves as {@link #FluxOnAssembly(Flux, String)}.
	 */
	FluxOnAssembly(Flux<? extends T> source, @Nullable String description, boolean light) {
		super(source);
		if (light) {
			this.snapshotStack = new AssemblyLightSnapshotException(description);
		}
		else {
			this.snapshotStack = new AssemblySnapshotException(description);
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb = sb.append('{')
			   .append(" \"operator\" : ")
			   .append('"')
			   .append(getClass().getSimpleName()
								 .replaceAll("Flux", ""))
			   .append('"');
		if (snapshotStack != null) {
			sb = sb.append(", ")
				   .append(" \"description\" : ")
				   .append('"')
				   .append(snapshotStack.getMessage())
				   .append('"');
		}
		return sb.append(' ')
				 .append('}')
				 .toString();
	}

	static String getStacktrace(
			@Nullable ContextualPublisher<?> source,
			AssemblySnapshotException snapshotStack) {
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
				if (row.contains("OnOperatorHook")) {
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
		if (ase.isLight()) {
			sb.append("\nAssembly site of producer [")
			  .append(sourceClass.getName())
			  .append("] is identified by light checkpoint [")
			  .append(ase.getMessage())
			  .append("].");
			return;
		}

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
	static <T> void subscribe(Subscriber<? super T> s,
			ContextualPublisher<? extends T> source,
			@Nullable AssemblySnapshotException snapshotStack,
			Context ctx) {

		if(snapshotStack != null) {
			if (s instanceof ConditionalSubscriber) {
				ConditionalSubscriber<? super T> cs = (ConditionalSubscriber<? super T>) s;
				source.subscribe(new OnAssemblyConditionalSubscriber<>(cs,
						snapshotStack,
						source), ctx);
			}
			else {
				source.subscribe(new OnAssemblySubscriber<>(s, snapshotStack, source),
						ctx);
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
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		subscribe(s, source, snapshotStack, ctx);
	}

	/**
	 * The exception that captures assembly context, possibly with a user-readable
	 * description or a wider correlation ID (which serves as the exception's
	 * {@link #getMessage() message}). Use the empty constructor if the later is not
	 * relevant.
	 */
	static class AssemblySnapshotException extends RuntimeException {

		final boolean checkpointed;

		AssemblySnapshotException() {
			super();
			this.checkpointed = false;
		}

		/**
		 * @param description a description for the assembly traceback.
		 * Use {@link #AssemblySnapshotException()} rather than null if not relevant.
		 */
		AssemblySnapshotException(@Nullable String description) {
			super(description);
			this.checkpointed = true;
		}

		public boolean isLight() {
			return false;
		}
	}

	static final class AssemblyLightSnapshotException extends AssemblySnapshotException {

		public AssemblyLightSnapshotException(@Nullable String description) {
			super(description);
		}

		@Override
		public synchronized Throwable fillInStackTrace() {
			return this; //intentionally NO-OP
		}

		@Override
		public boolean isLight() {
			return true;
		}
	}

	/**
	 * The holder for the assembly stacktrace (as its message).
	 */
	static final class OnAssemblyException extends RuntimeException {

		final List<Tuple3<Integer, String, Integer>> chainOrder = new LinkedList<>();

		/** */
		private static final long serialVersionUID = 5278398300974016773L;

		OnAssemblyException(ContextualPublisher<?> parent, AssemblySnapshotException ase, String message) {
			super(message);
			//skip the "error seen by" if light (no stack)
			if (!ase.isLight()) {
				chainOrder.add(Tuples.of(parent.hashCode(), extract(message, true), 0));
			}
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

		void add(ContextualPublisher<?> parent, String stacktrace) {
			//noinspection ConstantConditions
			int key = getParentOrThis(Scannable.from(parent));
			synchronized (chainOrder) {
				int i = 0;

				int n = chainOrder.size();
				int j = n - 1;
				Tuple3<Integer, String, Integer> tmp;
				while(j >= 0){
					tmp = chainOrder.get(j);
					//noinspection ConstantConditions
					if(tmp.getT1() == key){
						//noinspection ConstantConditions
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
			//skip the "error has been observed" traceback if mapped traceback is empty
			synchronized (chainOrder) {
				if (chainOrder.isEmpty()) {
					return super.getMessage();
				}

				StringBuilder sb = new StringBuilder(super.getMessage()).append(
						"Error has been observed by the following operator(s):\n");
				for(Tuple3<Integer, String, Integer> t : chainOrder) {
					//noinspection ConstantConditions
					mapLine(t.getT3(), sb, t.getT2());
				}
				return sb.toString();
			}
		}
	}

	static int getParentOrThis(Scannable parent) {
		return parent.parents()
		             .filter(s -> !(s instanceof AssemblyOp))
		             .findFirst()
		             .map(Object::hashCode)
		             .orElse(parent.hashCode());
	}

	static class OnAssemblySubscriber<T> extends CachedContextProducer<T>
			implements InnerOperator<T, T>, QueueSubscription<T> {

		final AssemblySnapshotException snapshotStack;
		final ContextualPublisher<?>    parent;

		QueueSubscription<T> qs;
		Subscription         s;
		int                  fusionMode;

		OnAssemblySubscriber(Subscriber<? super T> actual,
				AssemblySnapshotException snapshotStack, ContextualPublisher<?> parent) {
			super(actual);
			this.snapshotStack = snapshotStack;
			this.parent = parent;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;

			return InnerOperator.super.scanUnsafe(key);
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
			OnAssemblyException set = null;
			if (t.getSuppressed().length > 0) {
				for (Throwable e : t.getSuppressed()) {
					if (e instanceof OnAssemblyException) {
						OnAssemblyException oae = ((OnAssemblyException) e);
						oae.add(parent, getStacktrace(parent, snapshotStack));
						set = oae;
						break;
					}
				}
			}
			if (set == null) {
				t.addSuppressed(new OnAssemblyException(parent, snapshotStack,
						getStacktrace(parent, snapshotStack)));
			}
			else if(snapshotStack.checkpointed && t != snapshotStack){
				t.addSuppressed(snapshotStack);
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
		@Nullable
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
				AssemblySnapshotException stacktrace, ContextualPublisher<?> parent) {
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