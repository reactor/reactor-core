/*
 * Copyright (c) 2022-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.AbstractQueue;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.micrometer.context.ContextAccessor;
import io.micrometer.context.ContextRegistry;
import io.micrometer.context.ContextSnapshot;

import io.micrometer.context.ContextSnapshotFactory;
import io.micrometer.context.ThreadLocalAccessor;
import reactor.core.observability.SignalListener;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * Utility private class to detect if the <a href="https://github.com/micrometer-metrics/context-propagation">context-propagation library</a> is on the classpath and to offer
 * ContextSnapshot support to {@link Flux} and {@link Mono}.
 *
 * @author Simon Baslé
 */
final class ContextPropagation {

	static final Function<Context, Context> NO_OP = c -> c;
	static final Function<Context, Context> WITH_GLOBAL_REGISTRY_NO_PREDICATE;

	static ContextSnapshotFactory globalContextSnapshotFactory = null;

	static {
		WITH_GLOBAL_REGISTRY_NO_PREDICATE = ContextPropagationSupport.isContextPropagationAvailable() ?
				new ContextCaptureNoPredicate() : NO_OP;

		if (ContextPropagationSupport.isContextPropagation103Available()) {
			globalContextSnapshotFactory = ContextSnapshotFactory.builder()
			                                                     .clearMissing(false)
			                                                     .build();
		}
	}

	static void configureContextSnapshotFactory(boolean clearMissing) {
		if (ContextPropagationSupport.isContextPropagation103OnClasspath) {
			globalContextSnapshotFactory = ContextSnapshotFactory.builder()
			                                                     .clearMissing(clearMissing)
			                                                     .build();
		}
	}

	@SuppressWarnings("unchecked")
	static <C> ContextSnapshot.Scope setThreadLocals(Object context) {
		if (ContextPropagationSupport.isContextPropagation103OnClasspath) {
			return globalContextSnapshotFactory.setThreadLocalsFrom(context);
		}
		else {
			ContextRegistry registry = ContextRegistry.getInstance();
			ContextAccessor<?, ?> contextAccessor = registry.getContextAccessorForRead(context);
			Map<Object, Object> previousValues = null;
			for (ThreadLocalAccessor<?> threadLocalAccessor : registry.getThreadLocalAccessors()) {
				Object key = threadLocalAccessor.key();
				Object value = ((ContextAccessor<C, ?>) contextAccessor).readValue((C) context, key);
				previousValues = setThreadLocal(key, value, threadLocalAccessor, previousValues);
			}
			if (ContextPropagationSupport.isContextPropagation101Available()) {
				return ReactorScopeImpl.from(previousValues, registry);
			}
			return ReactorScopeImpl100.from(previousValues, registry);
		}
	}

	@SuppressWarnings("unchecked")
	private static <V> Map<Object, Object> setThreadLocal(Object key, @Nullable V value,
			ThreadLocalAccessor<?> accessor, @Nullable Map<Object, Object> previousValues) {

		previousValues = (previousValues != null ? previousValues : new HashMap<>());
		previousValues.put(key, accessor.getValue());
		if (value != null) {
			((ThreadLocalAccessor<V>) accessor).setValue(value);
		}
		else {
			accessor.reset();
		}
		return previousValues;
	}

	static ContextSnapshot captureThreadLocals() {
		if (ContextPropagationSupport.isContextPropagation103OnClasspath) {
			return globalContextSnapshotFactory.captureAll();
		}
		else {
			return ContextSnapshot.captureAll();
		}
	}

	public static Function<Runnable, Runnable> scopePassingOnScheduleHook() {
		return delegate -> {
			ContextSnapshot contextSnapshot = captureThreadLocals();
			return contextSnapshot.wrap(delegate);
		};
	}

	/**
	 * Create a support function that takes a snapshot of thread locals and merges them with the
	 * provided {@link Context}, resulting in a new {@link Context} which includes entries
	 * captured from threadLocals by the Context-Propagation API.
	 * <p>
	 * This variant uses the implicit global {@code ContextRegistry} and captures from all
	 * available {@code ThreadLocalAccessors}. It is the same variant backing {@link Flux#contextCapture()}
	 * and {@link Mono#contextCapture()}.
	 *
	 * @return the {@link Context} augmented with captured entries
	 */
	static Function<Context, Context> contextCapture() {
		return WITH_GLOBAL_REGISTRY_NO_PREDICATE;
	}

	static Context contextCaptureToEmpty() {
		return contextCapture().apply(Context.empty());
	}

	/**
	 * When <a href="https://github.com/micrometer-metrics/context-propagation">context-propagation library</a>
	 * is available on the classpath, the provided {@link BiConsumer handler} will be
	 * called with {@link ThreadLocal} values restored from the provided {@link Context}.
	 * @param handler user provided handler
	 * @param contextSupplier supplies the potentially modified {@link Context} to
	 *                           restore {@link ThreadLocal} values from
	 * @return potentially wrapped {@link BiConsumer} or the original
	 * @param <T> type of handled values
	 * @param <R> the transformed type
	 */
	static <T, R> BiConsumer<T, SynchronousSink<R>> contextRestoreForHandle(BiConsumer<T, SynchronousSink<R>> handler, Supplier<Context> contextSupplier) {
		if (ContextPropagationSupport.shouldRestoreThreadLocalsInSomeOperators()) {
			final Context ctx = contextSupplier.get();
			if (ctx.isEmpty()) {
				return handler;
			}

			if (ContextPropagationSupport.isContextPropagation103OnClasspath) {
				return (v, sink) -> {
					try (ContextSnapshot.Scope ignored = globalContextSnapshotFactory.setThreadLocalsFrom(ctx)) {
						handler.accept(v, sink);
					}
				};
			}
			else {
				return (v, sink) -> {
					try (ContextSnapshot.Scope ignored = ContextSnapshot.setAllThreadLocalsFrom(ctx)) {
						handler.accept(v, sink);
					}
				};
			}
		}
		else {
			return handler;
		}
	}

	/**
	 * When <a href="https://github.com/micrometer-metrics/context-propagation">context-propagation library</a>
	 * is available on the classpath, the provided {@link SignalListener} will be wrapped
	 * with another one that restores {@link ThreadLocal} values from the provided
	 * {@link Context}.
	 * <p><strong>Note, this is only applied to {@link FluxTap}, {@link FluxTapFuseable},
	 * {@link MonoTap}, and {@link MonoTapFuseable}.</strong> The automatic propagation
	 * variants: {@link FluxTapRestoringThreadLocals} and
	 * {@link MonoTapRestoringThreadLocals} do not use this method.
	 * @param original the original {@link SignalListener} from the user
	 * @param contextSupplier supplies the potentially modified {@link Context} to
	 *                           restore {@link ThreadLocal} values from
	 * @return potentially wrapped {@link SignalListener} or the original
	 * @param <T> type of handled values
	 */
	static <T> SignalListener<T> contextRestoreForTap(final SignalListener<T> original, Supplier<Context> contextSupplier) {
		if (!ContextPropagationSupport.isContextPropagationAvailable()) {
			return original;
		}
		final Context ctx = contextSupplier.get();
		if (ctx.isEmpty()) {
			return original;
		}

		if (ContextPropagationSupport.isContextPropagation103OnClasspath) {
			return new ContextRestore103SignalListener<>(original, ctx, globalContextSnapshotFactory);
		}
		else {
			return new ContextRestoreSignalListener<>(original, ctx);
		}
	}

	//the SignalListener implementation can be tested independently with a test-specific ContextRegistry
	static class ContextRestoreSignalListener<T> implements SignalListener<T> {

		final SignalListener<T> original;
		final ContextView context;

		public ContextRestoreSignalListener(SignalListener<T> original,
				ContextView context) {
			this.original = original;
			this.context = context;
		}

		ContextSnapshot.Scope restoreThreadLocals() {
			return ContextSnapshot.setAllThreadLocalsFrom(this.context);
		}

		@Override
		public final void doFirst() throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doFirst();
			}
		}

		@Override
		public final void doFinally(SignalType terminationType) throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doFinally(terminationType);
			}
		}

		@Override
		public final void doOnSubscription() throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doOnSubscription();
			}
		}

		@Override
		public final void doOnFusion(int negotiatedFusion) throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doOnFusion(negotiatedFusion);
			}
		}

		@Override
		public final void doOnRequest(long requested) throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doOnRequest(requested);
			}
		}

		@Override
		public final void doOnCancel() throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doOnCancel();
			}
		}

		@Override
		public final void doOnNext(T value) throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doOnNext(value);
			}
		}

		@Override
		public final void doOnComplete() throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doOnComplete();
			}
		}

		@Override
		public final void doOnError(Throwable error) throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doOnError(error);
			}
		}

		@Override
		public final void doAfterComplete() throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doAfterComplete();
			}
		}

		@Override
		public final void doAfterError(Throwable error) throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doAfterError(error);
			}
		}

		@Override
		public final void doOnMalformedOnNext(T value) throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doOnMalformedOnNext(value);
			}
		}

		@Override
		public final void doOnMalformedOnError(Throwable error) throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doOnMalformedOnError(error);
			}
		}

		@Override
		public final void doOnMalformedOnComplete() throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doOnMalformedOnComplete();
			}
		}

		@Override
		public final void handleListenerError(Throwable listenerError) {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.handleListenerError(listenerError);
			}
		}

		@Override
		public final Context addToContext(Context originalContext) {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				return original.addToContext(originalContext);
			}
		}
	}

	//the SignalListener implementation can be tested independently with a test-specific ContextRegistry
	static final class ContextRestore103SignalListener<T> extends ContextRestoreSignalListener<T> {

		final ContextSnapshotFactory contextSnapshotFactory;

		public ContextRestore103SignalListener(SignalListener<T> original,
				ContextView context, ContextSnapshotFactory contextSnapshotFactory) {
			super(original, context);
			this.contextSnapshotFactory = contextSnapshotFactory;
		}

		ContextSnapshot.Scope restoreThreadLocals() {
			return contextSnapshotFactory.setThreadLocalsFrom(this.context);
		}
	}

	static final class ContextCaptureNoPredicate implements Function<Context, Context> {

		@Override
		public Context apply(Context context) {
			return captureThreadLocals().updateContext(context);
		}
	}

	static final class ContextQueue<T> extends AbstractQueue<T> {

		static final String NOT_SUPPORTED_MESSAGE = "ContextQueue wrapper is intended " +
				"for use with instances returned by Queues class. Iterator based " +
				"methods are usually unsupported.";

		final Queue<Envelope<T>> envelopeQueue;

		boolean cleanOnNull;
		boolean hasPrevious = false;

		Thread                lastReader;
		ContextSnapshot.Scope scope;

		@SuppressWarnings({"unchecked", "rawtypes"})
		ContextQueue(Queue<?> queue) {
			this.envelopeQueue = (Queue) queue;
		}

		@Override
		public int size() {
			return envelopeQueue.size();
		}

		@Override
		public boolean offer(T o) {
			ContextSnapshot contextSnapshot = captureThreadLocals();
			return envelopeQueue.offer(new Envelope<>(o, contextSnapshot));
		}

		@Override
		public T poll() {
			Envelope<T> envelope = envelopeQueue.poll();
			if (envelope == null) {
				if (cleanOnNull && scope != null) {
					// clear thread-locals if they were just restored
					scope.close();
				}
				cleanOnNull = true;
				lastReader = Thread.currentThread();
				hasPrevious = false;
				return null;
			}


			restoreTheContext(envelope);
			hasPrevious = true;
			return envelope.body;
		}

		private void restoreTheContext(Envelope<T> envelope) {
			ContextSnapshot contextSnapshot = envelope.contextSnapshot;
			// tries to read existing Thread for existing ThreadLocals
			ContextSnapshot currentContextSnapshot = captureThreadLocals();
			if (!contextSnapshot.equals(currentContextSnapshot)) {
				if (!hasPrevious || !Thread.currentThread().equals(this.lastReader)) {
					// means context was restored form the envelope,
					// thus it has to be cleared
					cleanOnNull = true;
					lastReader = Thread.currentThread();
				}
				scope = contextSnapshot.setThreadLocals();
			}
			else if (!hasPrevious || !Thread.currentThread().equals(this.lastReader)) {
				// means same context was already available, no need to clean anything
				cleanOnNull = false;
				lastReader = Thread.currentThread();
			}
		}

		@Override
		@Nullable
		public T peek() {
			Envelope<T> envelope = envelopeQueue.peek();
			return envelope == null ? null : envelope.body;
		}

		@Override
		public Iterator<T> iterator() {
			throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
		}

	}

	static class Envelope<T> {

		final T               body;
		final ContextSnapshot contextSnapshot;

		Envelope(T body, ContextSnapshot contextSnapshot) {
			this.body = body;
			this.contextSnapshot = contextSnapshot;
		}
	}

	private static class ReactorScopeImpl implements ContextSnapshot.Scope {

		private final Map<Object, Object> previousValues;

		private final ContextRegistry contextRegistry;

		private ReactorScopeImpl(Map<Object, Object> previousValues,
				ContextRegistry contextRegistry) {
			this.previousValues = previousValues;
			this.contextRegistry = contextRegistry;
		}

		@Override
		public void close() {
			for (ThreadLocalAccessor<?> accessor : this.contextRegistry.getThreadLocalAccessors()) {
				if (this.previousValues.containsKey(accessor.key())) {
					Object previousValue = this.previousValues.get(accessor.key());
					resetThreadLocalValue(accessor, previousValue);
				}
			}
		}

		@SuppressWarnings("unchecked")
		private <V> void resetThreadLocalValue(ThreadLocalAccessor<?> accessor, @Nullable V previousValue) {
			if (previousValue != null) {
				((ThreadLocalAccessor<V>) accessor).restore(previousValue);
			}
			else {
				accessor.reset();
			}
		}

		public static ContextSnapshot.Scope from(@Nullable Map<Object, Object> previousValues, ContextRegistry registry) {
			return (previousValues != null ? new ReactorScopeImpl(previousValues, registry) : () -> {
			});
		}
	}

	private static class ReactorScopeImpl100 implements ContextSnapshot.Scope {

		private final Map<Object, Object> previousValues;

		private final ContextRegistry contextRegistry;

		private ReactorScopeImpl100(Map<Object, Object> previousValues,
				ContextRegistry contextRegistry) {
			this.previousValues = previousValues;
			this.contextRegistry = contextRegistry;
		}

		@Override
		public void close() {
			for (ThreadLocalAccessor<?> accessor : this.contextRegistry.getThreadLocalAccessors()) {
				if (this.previousValues.containsKey(accessor.key())) {
					Object previousValue = this.previousValues.get(accessor.key());
					resetThreadLocalValue(accessor, previousValue);
				}
			}
		}

		@SuppressWarnings("unchecked")
		private <V> void resetThreadLocalValue(ThreadLocalAccessor<?> accessor, @Nullable V previousValue) {
			if (previousValue != null) {
				((ThreadLocalAccessor<V>) accessor).setValue(previousValue);
			}
			else {
				accessor.reset();
			}
		}

		public static ContextSnapshot.Scope from(@Nullable Map<Object, Object> previousValues, ContextRegistry registry) {
			return (previousValues != null ? new ReactorScopeImpl100(previousValues, registry) : () -> {
			});
		}
	}
}
