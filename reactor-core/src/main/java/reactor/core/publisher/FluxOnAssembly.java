/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.function.Tuple4;
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
final class FluxOnAssembly<T> extends FluxOperator<T, T> implements Fuseable,
                                                                    AssemblyOp {

	final AssemblySnapshot snapshotStack;

	/**
	 * Create an assembly trace decorated as a {@link Flux}.
	 */
	FluxOnAssembly(Flux<? extends T> source, AssemblySnapshot snapshotStack) {
		super(source);
		this.snapshotStack = snapshotStack;
	}

	@Override
	public String stepName() {
		return snapshotStack.operatorAssemblyInformation();
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.ACTUAL_METADATA) return !snapshotStack.checkpointed;

		return super.scanUnsafe(key);
	}

	@Override
	public String toString() {
		return snapshotStack.operatorAssemblyInformation();
	}

	static void fillStacktraceHeader(StringBuilder sb, Class<?> sourceClass, @Nullable String description) {
		sb.append("\nAssembly trace from producer [")
		  .append(sourceClass.getName())
		  .append("]");

		if (description != null) {
			sb.append(", described as [")
			  .append(description)
			  .append("]");
		}
		sb.append(" :\n");
	}

	@SuppressWarnings("unchecked")
	static <T> void subscribe(CoreSubscriber<? super T> s,
			Flux<? extends T> source,
			@Nullable AssemblySnapshot snapshotStack) {

		if(snapshotStack != null) {
			if (s instanceof ConditionalSubscriber) {
				ConditionalSubscriber<? super T> cs = (ConditionalSubscriber<? super T>) s;
				source.subscribe(new OnAssemblyConditionalSubscriber<>(cs,
						snapshotStack,
						source));
			}
			else {
				source.subscribe(new OnAssemblySubscriber<>(s, snapshotStack, source));
			}
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(CoreSubscriber<? super T> actual) {
		if(snapshotStack != null) {
			if (actual instanceof ConditionalSubscriber) {
				ConditionalSubscriber<? super T> cs = (ConditionalSubscriber<? super T>) actual;
				source.subscribe(new OnAssemblyConditionalSubscriber<>(cs,
						snapshotStack,
						source));
			}
			else {
				source.subscribe(new OnAssemblySubscriber<>(actual, snapshotStack, source));
			}
		}
	}

	/**
	 * A snapshot of current assembly traceback, possibly with a user-readable
	 * description or a wider correlation ID.
	 */
	static class AssemblySnapshot {

		final boolean          checkpointed;
		@Nullable
		final String           description;
		final Supplier<String> assemblyInformationSupplier;
		String cached;

		/**
		 * @param description a description for the assembly traceback.
		 * @param assemblyInformationSupplier a callSite supplier.
		 */
		AssemblySnapshot(@Nullable String description, Supplier<String> assemblyInformationSupplier) {
			this(description != null, description, assemblyInformationSupplier);
		}

		private AssemblySnapshot(boolean checkpointed, @Nullable String description, Supplier<String> assemblyInformationSupplier) {
			this.checkpointed = checkpointed;
			this.description = description;
			this.assemblyInformationSupplier = assemblyInformationSupplier;
		}

		@Nullable
		public String getDescription() {
			return description;
		}

		public boolean isLight() {
			return false;
		}

		String toAssemblyInformation() {
			if(cached == null) {
				cached = assemblyInformationSupplier.get();
			}
			return cached;
		}

		String operatorAssemblyInformation() {
			return Traces.extractOperatorAssemblyInformation(toAssemblyInformation());
		}
	}

	static final class AssemblyLightSnapshot extends AssemblySnapshot {

		AssemblyLightSnapshot(@Nullable String description) {
			super(true, description, () -> "");
			cached = "checkpoint(\"" + description + "\")";
		}

		@Override
		public boolean isLight() {
			return true;
		}

		@Override
		String operatorAssemblyInformation() {
			return cached;
		}
	}

	/**
	 * The holder for the assembly stacktrace (as its message).
	 */
	static final class OnAssemblyException extends RuntimeException {

		final List<Tuple4<Integer, String, String, Integer>> chainOrder = new LinkedList<>();

		/** */
		private static final long serialVersionUID = 5278398300974016773L;

		OnAssemblyException(Publisher<?> parent, AssemblySnapshot ase, String message) {
			super(message);
			//skip the "error seen by" if light (no stack)
			if (!ase.isLight()) {
				chainOrder.add(toTuple(parent, Traces.extractOperatorAssemblyInformationParts(message, true), 0));
			}
		}

		@Override
		public Throwable fillInStackTrace() {
			return this;
		}

		Tuple4<Integer, String, String, Integer> toTuple(Publisher<?> parent, String[] callSite, int depth) {
			if (callSite.length == 2) {
				return Tuples.of(parent.hashCode(), callSite[0], callSite[1], depth);
			}
			else {
				return Tuples.of(parent.hashCode(), "checkpoint", callSite[0], depth);
			}
		}

		void add(Publisher<?> parent, String[] callSite) {
			//noinspection ConstantConditions
			int key = getParentOrThis(Scannable.from(parent));
			synchronized (chainOrder) {
				int i = 0;

				int n = chainOrder.size();
				int j = n - 1;
				Tuple4<Integer, String, String, Integer> tmp;
				while(j >= 0){
					tmp = chainOrder.get(j);
					//noinspection ConstantConditions
					if(tmp.getT1() == key){
						//noinspection ConstantConditions
						i = tmp.getT4();
						break;
					}
					j--;
				}


				for(;;){
					Tuple4<Integer, String, String, Integer> t = toTuple(parent, callSite, i);

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

				int maxWidth = 0;
				for (Tuple4<Integer, String, String, Integer> t : chainOrder) {
					int length = t.getT2().length();
					if (length > maxWidth) {
						maxWidth = length;
					}
				}

				StringBuilder sb = new StringBuilder(super.getMessage())
						.append("\nError has been observed at the following site(s):\n");
				for(Tuple4<Integer, String, String, Integer> t : chainOrder) {
					Integer indent = t.getT4();
					String operator = t.getT2();
					String message = t.getT3();
					sb.append("\t|_");
					for (int i = 0; i < indent; i++) {
						sb.append("____");
					}

					for (int i = operator.length(); i < maxWidth + 1; i++) {
						sb.append(' ');
					}
					sb.append(operator);
					sb.append(Traces.CALL_SITE_GLUE);
					sb.append(message);
					sb.append("\n");
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

	static class OnAssemblySubscriber<T>
			implements InnerOperator<T, T>, QueueSubscription<T> {

		final AssemblySnapshot          snapshotStack;
		final Publisher<?>              parent;
		final CoreSubscriber<? super T> actual;

		QueueSubscription<T> qs;
		Subscription         s;
		int                  fusionMode;

		OnAssemblySubscriber(CoreSubscriber<? super T> actual,
				AssemblySnapshot snapshotStack, Publisher<?> parent) {
			this.actual = actual;
			this.snapshotStack = snapshotStack;
			this.parent = parent;
		}

		@Override
		public final CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.ACTUAL_METADATA) return !snapshotStack.checkpointed;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public String toString() {
			return snapshotStack.operatorAssemblyInformation();
		}

		@Override
		public String stepName() {
			return toString();
		}

		@Override
		final public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		final public void onError(Throwable t) {
			actual.onError(fail(t));
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

		final Throwable fail(Throwable t) {
			boolean lightCheckpoint = snapshotStack.isLight();

			OnAssemblyException onAssemblyException = null;
			for (Throwable e : t.getSuppressed()) {
				if (e instanceof OnAssemblyException) {
					onAssemblyException = (OnAssemblyException) e;
					break;
				}
			}

			if (onAssemblyException != null) {
				final String[] backtrace;
				if (lightCheckpoint) {
					backtrace = new String[] { snapshotStack.getDescription() };
				}
				else {
					String assemblyInformation = snapshotStack.toAssemblyInformation();
					backtrace = Traces.extractOperatorAssemblyInformationParts(assemblyInformation, false);
				}

				onAssemblyException.add(parent, backtrace);

				return t;
			}

			if (lightCheckpoint) {
				onAssemblyException = new OnAssemblyException(parent, snapshotStack, "");
				onAssemblyException.add(parent, new String[] { snapshotStack.getDescription() });
			}
			else {
				StringBuilder sb = new StringBuilder();
				fillStacktraceHeader(sb, parent.getClass(), snapshotStack.getDescription());
				sb.append(snapshotStack.toAssemblyInformation().replaceFirst("\\n$", ""));
				String description = sb.toString();
				onAssemblyException = new OnAssemblyException(parent, snapshotStack, description);
			}

			return Exceptions.addSuppressed(t, onAssemblyException);
		}

		@Override
		final public boolean isEmpty() {
			try {
				return qs.isEmpty();
			}
			catch (Throwable ex) {
				Exceptions.throwIfFatal(ex);
				throw Exceptions.propagate(fail(ex));
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
				throw Exceptions.propagate(fail(ex));
			}
		}
	}

	static final class OnAssemblyConditionalSubscriber<T> extends OnAssemblySubscriber<T>
			implements ConditionalSubscriber<T> {

		final ConditionalSubscriber<? super T> actualCS;

		OnAssemblyConditionalSubscriber(ConditionalSubscriber<? super T> actual,
				AssemblySnapshot stacktrace, Publisher<?> parent) {
			super(actual, stacktrace, parent);
			this.actualCS = actual;
		}

		@Override
		public boolean tryOnNext(T t) {
			return actualCS.tryOnNext(t);
		}

	}

}

interface AssemblyOp {}