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

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
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
final class FluxOnAssembly<T> extends InternalFluxOperator<T, T> implements Fuseable,
                                                                    AssemblyOp {

	final AssemblySnapshot snapshotStack;
	final boolean withDebugStats;

	/**
	 * Create an assembly trace decorated as a {@link Flux}.
	 */
	FluxOnAssembly(Flux<? extends T> source, AssemblySnapshot snapshotStack, boolean withDebugStats) {
		super(source);
		this.snapshotStack = snapshotStack;
		this.withDebugStats = withDebugStats;
	}

	@Override
	public String stepName() {
		return snapshotStack.operatorAssemblyInformation();
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.ACTUAL_METADATA) return !snapshotStack.checkpointed;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

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
	static <T> CoreSubscriber<? super T> wrapSubscriber(CoreSubscriber<? super T> actual,
														Flux<? extends T> source,
														@Nullable AssemblySnapshot snapshotStack,
														@Nullable DebugStats debugStats) {
		if (snapshotStack != null) {
			if (actual instanceof ConditionalSubscriber) {
				ConditionalSubscriber<? super T> cs =
						(ConditionalSubscriber<? super T>) actual;
				return new OnAssemblyConditionalSubscriber<>(cs,
						snapshotStack,
						debugStats,
						source);
			}
			else {
				return new OnAssemblySubscriber<>(actual,
						snapshotStack,
						debugStats,
						source);
			}
		}
		else {
			return actual;
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return FluxOnAssembly.wrapSubscriber(actual,
				source,
				snapshotStack,
				withDebugStats ? new FluxOnAssembly.DefaultDebugStats() : null);
	}

	interface DebugStats {

		long requested();

		long produced();

		SignalType lastSignal();

		Instant lastRequestTime();

		Instant lastOnNextTime();

		boolean isCompleted();

		boolean isErrored();

		boolean isCancelled();

		void recordSignal(SignalType signalType);

		void recordRequest(long requested);

		void recordProduced();
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

		AssemblySnapshot(String assemblyInformation) {
			this.checkpointed = false;
			this.description = null;
			this.assemblyInformationSupplier = null;
			this.cached = assemblyInformation;
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

		public String lightPrefix() {
			return "";
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

	static final class DefaultDebugStats implements DebugStats {

		private static final long EPOCH_NANOS  = System.currentTimeMillis() * 1_000_000;
		private static final long NANO_START   = System.nanoTime();
		private static final long OFFSET_NANOS = EPOCH_NANOS - NANO_START;
		private static final long NANOS_PER_SECOND = 1_000_000_000;

		SignalType lastObservedSignal;

		long lastElapsedOnNextTimeNanos;
		long lastElapsedRequestTimeNanos;

		long produced;
		long requested;

		boolean   error;
		boolean   done;
		boolean   cancelled;

		@Override
		public boolean isCompleted() {
			return done && !error;
		}

		@Override
		public boolean isErrored() {
			return error;
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public long requested() {
			return requested;
		}

		@Override
		public long produced() {
			return produced;
		}

		@Override
		public SignalType lastSignal() {
			return lastObservedSignal;
		}

		@Override
		public Instant lastRequestTime() {
			return Instant.ofEpochSecond(lastElapsedRequestTimeNanos / NANOS_PER_SECOND,
					lastElapsedRequestTimeNanos % NANOS_PER_SECOND);
		}

		@Override
		public Instant lastOnNextTime() {
			return Instant.ofEpochSecond(lastElapsedOnNextTimeNanos / NANOS_PER_SECOND,
					lastElapsedOnNextTimeNanos % NANOS_PER_SECOND);
		}

		@Override
		public void recordSignal(SignalType signalType) {
			long currentNanoTime;

			switch (signalType) {
				case ON_SUBSCRIBE:
					this.lastObservedSignal = signalType;
					break;
				case ON_NEXT:
					currentNanoTime = nanoTime();
					this.lastElapsedOnNextTimeNanos = currentNanoTime;
					this.lastObservedSignal = signalType;
					break;
				case REQUEST:
					currentNanoTime = nanoTime();
					this.lastElapsedRequestTimeNanos = currentNanoTime;
					this.lastObservedSignal = signalType;
					break;
				case CANCEL:
					this.cancelled = true;
					break;
				case ON_ERROR:
					this.error = true;
				case ON_COMPLETE:
					this.done = true;
					break;
			}
		}

		private static long nanoTime() {
			return System.nanoTime() + OFFSET_NANOS;
		}

		@Override
		public void recordRequest(long requested) {
			this.recordSignal(SignalType.REQUEST);
			this.requested += requested;
		}

		@Override
		public void recordProduced() {
			this.recordSignal(SignalType.ON_NEXT);
			this.produced++;
		}
	}

	static final class AssemblyLightSnapshot extends AssemblySnapshot {

		AssemblyLightSnapshot(@Nullable String description) {
			super(true, description, null);
			cached = "checkpoint(\"" + description + "\")";
		}

		@Override
		public boolean isLight() {
			return true;
		}

		@Override
		public String lightPrefix() {
			return "checkpoint";
		}

		@Override
		String operatorAssemblyInformation() {
			return cached;
		}

	}

	static final class MethodReturnSnapshot extends AssemblySnapshot {

		MethodReturnSnapshot(String method) {
			super(true, method, null);
			cached = method;
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
	 *
	 * @implNote this package-private exception needs access to package-private enclosing class and methods,
	 * but it is also detected by {@link Exceptions#isTraceback(Throwable)} via {@link Class#getCanonicalName() reflection}.
	 * Be sure to update said method in case of a refactoring.
	 *
	 * @see Exceptions#isTraceback(Throwable)
	 */
	static final class OnAssemblyException extends RuntimeException {

		final List<Entry> chainOrder = new LinkedList<>();

		/** */
		private static final long serialVersionUID = 5278398300974016773L;

		OnAssemblyException(String message) {
			super(message);
		}

		@Override
		public Throwable fillInStackTrace() {
			return this;
		}

		void add(Publisher<?> parent, AssemblySnapshot snapshot, @Nullable DebugStats debugStats) {
			if (snapshot.isLight()) {
				add(parent, snapshot.lightPrefix(), snapshot.getDescription(), debugStats);
			}
			else {
				String assemblyInformation = snapshot.toAssemblyInformation();
				String[] parts = Traces.extractOperatorAssemblyInformationParts(assemblyInformation);
				if (parts.length > 0) {
					String prefix = parts.length > 1 ? parts[0] : "";
					String line = parts[parts.length - 1];

					add(parent, prefix, line, debugStats);
				}
			}
		}

		private void add(Publisher<?> parent, String prefix, String line, @Nullable DebugStats debugStats) {
			//noinspection ConstantConditions
			int key = getParentOrThis(Scannable.from(parent));
			synchronized (chainOrder) {
				int i = 0;

				int n = chainOrder.size();
				int j = n - 1;
				Entry tmp;
				while(j >= 0){
					tmp = chainOrder.get(j);
					//noinspection ConstantConditions
					if(tmp.key == key){
						//noinspection ConstantConditions
						i = tmp.indent;
						break;
					}
					j--;
				}


				for(;;){
					Entry t = new Entry(parent.hashCode(), i, prefix, line, debugStats);

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

				int maxOperatorWidth = 0;
				int maxRequestedWidth = 0;
				int maxProducedWidth = 0;
				for (Entry t : chainOrder) {
					int length = t.operator.length();
					if (length > maxOperatorWidth) {
						maxOperatorWidth = length;
					}

					final DebugStats stats = t.debugStats;
					if (stats != null) {
						final long requested = stats.requested();
						length = requested == Long.MAX_VALUE ? "∞".length() : String.valueOf(requested).length();

						if (length > maxRequestedWidth) {
							maxRequestedWidth = length;
						}

						length = String.valueOf(stats.produced()).length();

						if (length > maxProducedWidth) {
							maxProducedWidth = length;
						}
					}
				}

				StringBuilder sb = new StringBuilder(super.getMessage())
						.append("\nError has been observed at the following site(s):\n");
				for(Entry t : chainOrder) {
					int indent = t.indent;
					String operator = t.operator;
					String message = t.line;
					sb.append("\t|_");
					for (int i = 0; i < indent; i++) {
						sb.append("____");
					}

					for (int i = operator.length(); i < maxOperatorWidth + 1; i++) {
						sb.append(' ');
					}
					sb.append(operator);
					sb.append(Traces.CALL_SITE_GLUE);
					sb.append(message);
					final DebugStats stats = t.debugStats;
					if (stats != null) {
						sb.append("\n");
						for (int i = 0; i < indent; i++) {
							sb.append("    ");
						}
						for (int i = 0; i < maxOperatorWidth + 1; i++) {
							sb.append(' ');
						}

						sb.append("requested: ");
						String requested = stats.requested() == Long.MAX_VALUE ? "∞" : String.valueOf(stats.requested());
						for (int i = requested.length(); i < maxRequestedWidth + 1; i++) {
							sb.append(' ');
						}
						sb.append(requested);
						sb.append("; ");

						sb.append("produced: ");
						String produced = String.valueOf(stats.produced());
						for (int i = produced.length(); i < maxProducedWidth + 1; i++) {
							sb.append(' ');
						}
						sb.append(produced);
						sb.append("; ");

						sb.append("lastRequestAt: ");
						sb.append(DateTimeFormatter.ISO_INSTANT.format(stats.lastRequestTime()));
						sb.append("; ");

						sb.append("lastOnNextAt: ");
						sb.append(DateTimeFormatter.ISO_INSTANT.format(stats.lastOnNextTime()));
						sb.append("; ");

						sb.append("state: ");

						if (stats.isCompleted()) {
							sb.append("completed");
						} else if (stats.isErrored()) {
							sb.append("  errored");
						} else if (stats.isCancelled()) {
							sb.append("cancelled");
						} else {
							sb.append("  running");
						}
					}
					sb.append("\n");
				}
				sb.append("Stack trace:");
				return sb.toString();
			}
		}

		static class Entry {

			final int        key;
			final int        indent;
			final String     operator;
			final String     line;
			@Nullable
			final DebugStats debugStats;

			Entry(int key,
					int indent,
					String operator,
					String line,
					@Nullable DebugStats stats) {
				this.key = key;
				this.indent = indent;
				this.operator = operator;
				this.line = line;
				debugStats = stats;
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
		@Nullable
		final DebugStats                debugStats;
		final Publisher<?>              parent;
		final CoreSubscriber<? super T> actual;

		QueueSubscription<T> qs;
		Subscription         s;
		int                  fusionMode;

		OnAssemblySubscriber(CoreSubscriber<? super T> actual,
				AssemblySnapshot snapshotStack, @Nullable DebugStats debugStats, Publisher<?> parent) {
			this.actual = actual;
			this.snapshotStack = snapshotStack;
			this.debugStats = debugStats;
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
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

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
			final DebugStats debugStats = this.debugStats;
			if (debugStats != null) {
				debugStats.recordProduced();
			}
			actual.onNext(t);
		}

		@Override
		final public void onError(Throwable t) {
			final DebugStats debugStats = this.debugStats;
			if (debugStats != null) {
				debugStats.recordSignal(SignalType.ON_ERROR);
			}
			actual.onError(fail(t));
		}

		@Override
		final public void onComplete() {
			final DebugStats debugStats = this.debugStats;
			if (debugStats != null) {
				debugStats.recordSignal(SignalType.ON_COMPLETE);
			}
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

			if (onAssemblyException == null) {
				if (lightCheckpoint) {
					onAssemblyException = new OnAssemblyException("");
				}
				else {
					StringBuilder sb = new StringBuilder();
					fillStacktraceHeader(sb, parent.getClass(), snapshotStack.getDescription());
					sb.append(snapshotStack.toAssemblyInformation().replaceFirst("\\n$", ""));
					String description = sb.toString();
					onAssemblyException = new OnAssemblyException(description);
				}

				t = Exceptions.addSuppressed(t, onAssemblyException);
				final StackTraceElement[] stackTrace = t.getStackTrace();
				if (stackTrace.length > 0) {
					StackTraceElement[] newStackTrace = new StackTraceElement[stackTrace.length];
					int i = 0;
					for (StackTraceElement stackTraceElement : stackTrace) {
						String className = stackTraceElement.getClassName();

						if (className.startsWith("reactor.core.publisher.") && className.contains("OnAssembly")) {
							continue;
						}

						newStackTrace[i] = stackTraceElement;
						i++;
					}
					newStackTrace = Arrays.copyOf(newStackTrace, i);

					onAssemblyException.setStackTrace(newStackTrace);
					t.setStackTrace(new StackTraceElement[] {
							stackTrace[0]
					});
				}
			}

			onAssemblyException.add(parent, snapshotStack, debugStats);

			return t;
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

				final DebugStats debugStats = this.debugStats;
				if (debugStats != null) {
					debugStats.recordSignal(SignalType.ON_SUBSCRIBE);
				}

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
			final DebugStats debugStats = this.debugStats;
			if (debugStats != null) {
				debugStats.recordRequest(n);
			}
			s.request(n);
		}

		@Override
		final public void cancel() {
			final DebugStats debugStats = this.debugStats;
			if (debugStats != null) {
				debugStats.recordSignal(SignalType.CANCEL);
			}
			s.cancel();
		}

		@Override
		@Nullable
		final public T poll() {
			try {
				final T polled = qs.poll();
				final DebugStats debugStats = this.debugStats;
				if (polled != null && debugStats != null) {
					debugStats.recordProduced();
				}
				return polled;
			}
			catch (final Throwable ex) {
				Exceptions.throwIfFatal(ex);

				final DebugStats debugStats = this.debugStats;
				if (debugStats != null) {
					debugStats.recordSignal(SignalType.ON_ERROR);
				}
				throw Exceptions.propagate(fail(ex));
			}
		}
	}

	static final class OnAssemblyConditionalSubscriber<T> extends OnAssemblySubscriber<T>
			implements ConditionalSubscriber<T> {

		final ConditionalSubscriber<? super T> actualCS;

		OnAssemblyConditionalSubscriber(ConditionalSubscriber<? super T> actual,
				AssemblySnapshot stacktrace, @Nullable DebugStats debugStats,
				Publisher<?> parent) {
			super(actual, stacktrace, debugStats, parent);
			this.actualCS = actual;
		}

		@Override
		public boolean tryOnNext(T t) {
			final DebugStats debugStats = this.debugStats;
			if (debugStats != null) {
				debugStats.recordSignal(SignalType.ON_NEXT);

				final boolean produced = actualCS.tryOnNext(t);
				if (!produced) {
					debugStats.recordRequest(1);
					return false;
				}
				return true;
			}

			return actualCS.tryOnNext(t);
		}

	}

}

interface AssemblyOp {}
