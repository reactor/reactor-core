/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.publisher;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
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
final class FluxOnAssembly<T> extends InternalFluxOperator<T, T> implements Fuseable,
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
		if (key == Attr.ACTUAL_METADATA) return !snapshotStack.isCheckpoint;
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
														Publisher<?> current,
														@Nullable AssemblySnapshot snapshotStack) {
		if(snapshotStack != null) {
			if (actual instanceof ConditionalSubscriber) {
				ConditionalSubscriber<? super T> cs = (ConditionalSubscriber<? super T>) actual;
				return new OnAssemblyConditionalSubscriber<>(cs, snapshotStack, source, current);
			}
			else {
				return new OnAssemblySubscriber<>(actual, snapshotStack, source, current);
			}
		}
		else {
			return actual;
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return wrapSubscriber(actual, source, this, snapshotStack);
	}

	/**
	 * A snapshot of current assembly traceback, possibly with a user-readable
	 * description or a wider correlation ID.
	 */
	static class AssemblySnapshot {

		final boolean          isCheckpoint;
		@Nullable
		final String           description;
		@Nullable
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
			this.isCheckpoint = false;
			this.description = null;
			this.assemblyInformationSupplier = null;
			this.cached = assemblyInformation;
		}

		private AssemblySnapshot(boolean isCheckpoint, @Nullable String description,
								 @Nullable Supplier<String> assemblyInformationSupplier) {
			this.isCheckpoint = isCheckpoint;
			this.description = description;
			this.assemblyInformationSupplier = assemblyInformationSupplier;
		}

		public boolean hasDescription() {
			return this.description != null;
		}

		@Nullable
		public String getDescription() {
			return description;
		}

		public boolean isCheckpoint() {
			return this.isCheckpoint;
		}

		public boolean isLight() {
			return false;
		}

		public String lightPrefix() {
			return "";
		}

		String toAssemblyInformation() {
			if(cached == null) {
				if (assemblyInformationSupplier == null) {
					throw new IllegalStateException("assemblyInformation must either be supplied or resolvable");
				}
				cached = assemblyInformationSupplier.get();
			}
			return cached;
		}

		String operatorAssemblyInformation() {
			return Traces.extractOperatorAssemblyInformation(toAssemblyInformation());
		}
	}

	static final class CheckpointLightSnapshot extends AssemblySnapshot {

		CheckpointLightSnapshot(@Nullable String description) {
			super(true, description, null);
			this.cached = "checkpoint(\"" + (description == null ? "" : description) + "\")";
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

	static final class CheckpointHeavySnapshot extends AssemblySnapshot {

		CheckpointHeavySnapshot(@Nullable String description, Supplier<String> assemblyInformationSupplier) {
			super(true, description, assemblyInformationSupplier);
		}

		/**
		 * The lightPrefix is used despite the exception not being a light one (it will have a callsite supplier).
		 * @return the heavy checkpoint prefix, with description if relevant (eg. "checkpoint(heavy)")
		 */
		@Override
		public String lightPrefix() {
			return "checkpoint(" + (description == null ? "" : description) + ")";
		}
	}

	static final class MethodReturnSnapshot extends AssemblySnapshot {

		MethodReturnSnapshot(String method) {
			super(false, method, null);
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

	static final class ObservedAtInformationNode implements Serializable {
		private static final long serialVersionUID = 1L;

		final int id;
		final String operator;
		final String message;

		int occurrenceCounter;

		@Nullable
		ObservedAtInformationNode parent;
		Set<ObservedAtInformationNode> children;

		ObservedAtInformationNode(int id, String operator, String message) {
			this.id = id;
			this.operator = operator;
			this.message = message;
			this.occurrenceCounter = 0;
			this.children = new LinkedHashSet<>();
		}

		void incrementCount() {
			this.occurrenceCounter++;
		}

		void addNode(ObservedAtInformationNode node) {
			if (this == node) {
				return;
			}
			if (children.add(node)) {
				node.parent = this;
			}
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ObservedAtInformationNode node = (ObservedAtInformationNode) o;
			return id == node.id && operator.equals(node.operator) && message.equals(node.message);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, operator, message);
		}

		@Override
		public String toString() {
			return operator + "{" +
				"@" + id +
				(children.isEmpty() ? "" : ", " + children.size() + " children") +
				'}';
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

		/** */
		private static final long serialVersionUID = -6342981676020433721L;

		final Map<Integer, ObservedAtInformationNode> nodesPerId = new HashMap<>();
		final ObservedAtInformationNode root = new ObservedAtInformationNode(-1, "ROOT", "ROOT");
		int maxOperatorSize = 0;

		OnAssemblyException(String message) {
			super(message);
		}

		@Override
		public Throwable fillInStackTrace() {
			return this;
		}

		void add(Publisher<?> parent, Publisher<?> current, AssemblySnapshot snapshot) {
			if (snapshot.isCheckpoint()) {
				if (snapshot.isLight()) {
					add(parent, current, snapshot.lightPrefix(), Objects.requireNonNull(snapshot.getDescription()));
				}
				else {
					String assemblyInformation = snapshot.toAssemblyInformation();
					String[] parts = Traces.extractOperatorAssemblyInformationParts(assemblyInformation);

					if (parts.length > 0) {
						//we ignore the first part if there are two (classname and callsite). only use the line part
						String line = parts[parts.length - 1];
						add(parent, current, snapshot.lightPrefix(), line);
					}
					else {
						//should not happen with heavy checkpoints
						add(parent, current, snapshot.lightPrefix(), Objects.requireNonNull(snapshot.getDescription()));
					}
				}
			}
			else {
				String assemblyInformation = snapshot.toAssemblyInformation();
				String[] parts = Traces.extractOperatorAssemblyInformationParts(assemblyInformation);
				if (parts.length > 0) {
					String prefix = parts.length > 1 ? parts[0] : "";
					String line = parts[parts.length - 1];

					add(parent, current, prefix, line);
				}
			}
		}

		private void add(Publisher<?> operator, Publisher<?> currentAssembly, String prefix, String line) {
			Scannable parentAssembly = Scannable.from(currentAssembly)
				.parents()
				.filter(s -> s instanceof AssemblyOp)
				.findFirst()
				.orElse(null);

			int thisId = System.identityHashCode(currentAssembly);
			int parentId = System.identityHashCode(parentAssembly);

			ObservedAtInformationNode thisNode;
			synchronized (nodesPerId) {
				thisNode = nodesPerId.get(thisId);
				if (thisNode != null) {
					thisNode.incrementCount();
				}
				else {
					thisNode = new ObservedAtInformationNode(thisId, prefix, line);
					nodesPerId.put(thisId, thisNode);
				}

				if (parentAssembly == null) {
					root.addNode(thisNode);
				}
				else {
					ObservedAtInformationNode parentNode = nodesPerId.get(parentId);
					if (parentNode != null) {
						parentNode.addNode(thisNode);
					}
					else {
						root.addNode(thisNode);
					}
				}

				//pre-compute the maximum width of the operators
				int length = thisNode.operator.length();
				if (length > this.maxOperatorSize) {
					this.maxOperatorSize = length;
				}
			}
		}

		void findPathToLeaves(ObservedAtInformationNode node, List<List<ObservedAtInformationNode>> rootPaths) {
			if (node.children.isEmpty()) {
				List<ObservedAtInformationNode> pathForLeaf = new LinkedList<>();
				ObservedAtInformationNode traversed = node;
				while (traversed != null && traversed != root) {
					pathForLeaf.add(0, traversed);
					traversed = traversed.parent;
				}
				rootPaths.add(pathForLeaf);
				return;
			}
			node.children.forEach(n -> findPathToLeaves(n, rootPaths));
		}

		@Override
		public String getMessage() {
			//skip the "error has been observed" traceback if mapped traceback is empty
			synchronized (nodesPerId) {
				if (root.children.isEmpty()) {
					return super.getMessage();
				}

				StringBuilder sb = new StringBuilder(super.getMessage())
					.append(System.lineSeparator())
					.append("Error has been observed at the following site(s):")
					.append(System.lineSeparator());

				List<List<ObservedAtInformationNode>> rootPaths = new ArrayList<>();
				root.children.forEach(actualRoot -> findPathToLeaves(actualRoot, rootPaths));

				rootPaths.forEach(path -> path.forEach(node -> {
					boolean isRoot = node.parent == null || node.parent == root;
					sb.append("\t");
					String connector = "|_";
					if (isRoot) {
						connector = "*_";
					}
					sb.append(connector);

					char filler = isRoot ? '_' : ' ';
					for (int i = node.operator.length(); i < this.maxOperatorSize; i++) {
						sb.append(filler);
					}
					sb.append(filler);
					sb.append(node.operator);
					sb.append(Traces.CALL_SITE_GLUE);
					sb.append(node.message);
					if (node.occurrenceCounter > 0) {
						sb.append(" (observed ").append(node.occurrenceCounter + 1).append(" times)");
					}
					sb.append(System.lineSeparator());
				}));

				sb.append("Original Stack Trace:");
				return sb.toString();
			}
		}

		@Override
		public String toString() {
			String message = getLocalizedMessage();
			if (message == null) {
				return "The stacktrace should have been enhanced by Reactor, but there was no message in OnAssemblyException";
			}
			return "The stacktrace has been enhanced by Reactor, refer to additional information below: " + message;
		}
	}

	static class OnAssemblySubscriber<T>
			implements InnerOperator<T, T>, QueueSubscription<T> {

		final AssemblySnapshot          snapshotStack;
		final Publisher<?>              parent;
		final Publisher<?>              current;
		final CoreSubscriber<? super T> actual;

		QueueSubscription<T> qs;
		Subscription         s;
		int                  fusionMode;

		OnAssemblySubscriber(CoreSubscriber<? super T> actual,
							 AssemblySnapshot snapshotStack,
							 Publisher<?> parent,
							 Publisher<?> current) {
			this.actual = actual;
			this.snapshotStack = snapshotStack;
			this.parent = parent;
			this.current = current;
		}

		@Override
		public final CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.ACTUAL_METADATA) return !snapshotStack.isCheckpoint;
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

			onAssemblyException.add(parent, current, snapshotStack);

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
				AssemblySnapshot stacktrace, Publisher<?> parent, Publisher<?> current) {
			super(actual, stacktrace, parent, current);
			this.actualCS = actual;
		}

		@Override
		public boolean tryOnNext(T t) {
			return actualCS.tryOnNext(t);
		}

	}

}

interface AssemblyOp {}
