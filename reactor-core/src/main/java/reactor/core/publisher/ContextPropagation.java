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
import java.util.function.Predicate;
import java.util.function.Supplier;

import io.micrometer.context.ContextAccessor;
import io.micrometer.context.ContextRegistry;
import io.micrometer.context.ContextSnapshot;

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

	static final Predicate<Object> PREDICATE_TRUE = v -> true;
	static final Function<Context, Context> NO_OP = c -> c;
	static final Function<Context, Context> WITH_GLOBAL_REGISTRY_NO_PREDICATE;

	static {
		WITH_GLOBAL_REGISTRY_NO_PREDICATE = ContextPropagationSupport.isContextPropagationAvailable() ?
				new ContextCaptureNoPredicate(ContextRegistry.getInstance()) : NO_OP;
	}

	public static Function<Runnable, Runnable> scopePassingOnScheduleHook() {
		return delegate -> {
			ContextSnapshot contextSnapshot = ContextSnapshot.captureAll();
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

	/**
	 * Create a support function that takes a snapshot of thread locals and merges them with the
	 * provided {@link Context}, resulting in a new {@link Context} which includes entries
	 * captured from threadLocals by the Context-Propagation API.
	 * <p>
	 * The provided {@link Predicate} is used on keys associated to said thread locals
	 * by the Context-Propagation API to filter which entries should be captured in the
	 * first place.
	 * <p>
	 * This variant uses the implicit global {@code ContextRegistry} and captures only from
	 * available {@code ThreadLocalAccessors} that match the {@link Predicate}.
	 *
	 * @param captureKeyPredicate a {@link Predicate} used on keys to determine if each entry
	 * should be injected into the new {@link Context}
	 * @return a {@link Function} augmenting {@link Context} with captured entries
	 */
	static Function<Context, Context> contextCapture(Predicate<Object> captureKeyPredicate) {
		if (!ContextPropagationSupport.isContextPropagationOnClasspath) {
			return NO_OP;
		}
		return target -> ContextSnapshot.captureAllUsing(captureKeyPredicate, ContextRegistry.getInstance())
			.updateContext(target);
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
			return (v, sink) -> {
				try (ContextSnapshot.Scope ignored = ContextSnapshot.setAllThreadLocalsFrom(ctx)) {
					handler.accept(v, sink);
				}
			};
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
		return new ContextRestoreSignalListener<T>(original, ctx, null);
	}

	//the SignalListener implementation can be tested independently with a test-specific ContextRegistry
	static final class ContextRestoreSignalListener<T> implements SignalListener<T> {

		final SignalListener<T> original;
		final ContextView context;
		final ContextRegistry registry;

		public ContextRestoreSignalListener(SignalListener<T> original, ContextView context, @Nullable ContextRegistry registry) {
			this.original = original;
			this.context = context;
			this.registry = registry == null ? ContextRegistry.getInstance() : registry;
		}
		
		ContextSnapshot.Scope restoreThreadLocals() {
			return ContextSnapshot.setAllThreadLocalsFrom(this.context, this.registry);
		}

		@Override
		public void doFirst() throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doFirst();
			}
		}

		@Override
		public void doFinally(SignalType terminationType) throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doFinally(terminationType);
			}
		}

		@Override
		public void doOnSubscription() throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doOnSubscription();
			}
		}

		@Override
		public void doOnFusion(int negotiatedFusion) throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doOnFusion(negotiatedFusion);
			}
		}

		@Override
		public void doOnRequest(long requested) throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doOnRequest(requested);
			}
		}

		@Override
		public void doOnCancel() throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doOnCancel();
			}
		}

		@Override
		public void doOnNext(T value) throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doOnNext(value);
			}
		}

		@Override
		public void doOnComplete() throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doOnComplete();
			}
		}

		@Override
		public void doOnError(Throwable error) throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doOnError(error);
			}
		}

		@Override
		public void doAfterComplete() throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doAfterComplete();
			}
		}

		@Override
		public void doAfterError(Throwable error) throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doAfterError(error);
			}
		}

		@Override
		public void doOnMalformedOnNext(T value) throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doOnMalformedOnNext(value);
			}
		}

		@Override
		public void doOnMalformedOnError(Throwable error) throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doOnMalformedOnError(error);
			}
		}

		@Override
		public void doOnMalformedOnComplete() throws Throwable {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.doOnMalformedOnComplete();
			}
		}

		@Override
		public void handleListenerError(Throwable listenerError) {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				original.handleListenerError(listenerError);
			}
		}

		@Override
		public Context addToContext(Context originalContext) {
			try (ContextSnapshot.Scope ignored = restoreThreadLocals()) {
				return original.addToContext(originalContext);
			}
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
			ContextSnapshot contextSnapshot = ContextSnapshot.captureAll();
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
			ContextSnapshot currentContextSnapshot = ContextSnapshot.captureAll();
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

	static final class ContextCaptureNoPredicate implements Function<Context, Context> {
		final ContextRegistry globalRegistry;

		ContextCaptureNoPredicate(ContextRegistry globalRegistry) {
			this.globalRegistry = globalRegistry;
		}
		@Override
		public Context apply(Context context) {
			return ContextSnapshot.captureAllUsing(PREDICATE_TRUE, globalRegistry)
					.updateContext(context);
		}
	}

	/*
	 * Temporary methods not present in context-propagation library that allow
	 * clearing ThreadLocals not present in Reactor Context. Once context-propagation
	 * library adds the ability to do this, they can be removed from reactor-core.
	 */

	@SuppressWarnings("unchecked")
	static <C> ContextSnapshot.Scope setThreadLocals(Object context) {
		ContextRegistry registry = ContextRegistry.getInstance();
		ContextAccessor<?, ?> contextAccessor = registry.getContextAccessorForRead(context);
		Map<Object, Object> previousValues = null;
		for (ThreadLocalAccessor<?> threadLocalAccessor : registry.getThreadLocalAccessors()) {
			Object key = threadLocalAccessor.key();
			Object value = ((ContextAccessor<C, ?>) contextAccessor).readValue((C) context, key);
			previousValues = setThreadLocal(key, value, threadLocalAccessor, previousValues);
		}
		return ReactorScopeImpl.from(previousValues, registry);
	}

	@SuppressWarnings("unchecked")
	private static <V> Map<Object, Object> setThreadLocal(Object key, @Nullable V value,
			ThreadLocalAccessor<?> accessor, @Nullable Map<Object, Object> previousValues) {

		previousValues = (previousValues != null ? previousValues : new HashMap<>());
		previousValues.put(key, accessor.getValue());
		if (value != null) {
			((ThreadLocalAccessor<V>) accessor).setValue(value);
		} else {
			accessor.setValue();
		}
		return previousValues;
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
				accessor.restore();
			}
		}

		public static ContextSnapshot.Scope from(@Nullable Map<Object, Object> previousValues, ContextRegistry registry) {
			return (previousValues != null ? new ReactorScopeImpl(previousValues, registry) : () -> {
			});
		}
	}
}
