/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

import io.micrometer.context.ContextRegistry;
import io.micrometer.context.ContextSnapshot;

import reactor.core.CoreSubscriber;
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

	static final boolean isContextPropagationAvailable;

	static final String CAPTURED_CONTEXT_MARKER = "reactor.core.contextSnapshotCaptured";

	static final Predicate<Object> PREDICATE_TRUE = v -> true;
	static final Function<Context, Context> NO_OP = c -> c;
	static final Function<Context, Context> WITH_GLOBAL_REGISTRY_NO_PREDICATE;

	static {
		boolean contextPropagation;
		try {
			Class.forName("io.micrometer.context.ContextRegistry", false, ContextPropagation.class.getClassLoader());
			contextPropagation = true;
		}
		catch (Throwable t) {
			contextPropagation = false;
		}
		isContextPropagationAvailable = contextPropagation;
		if (contextPropagation) {
			WITH_GLOBAL_REGISTRY_NO_PREDICATE = new ContextCaptureFunction(PREDICATE_TRUE, ContextRegistry.getInstance());
		}
		else {
			WITH_GLOBAL_REGISTRY_NO_PREDICATE = NO_OP;
		}
	}

	/**
	 * Is Micrometer {@code context-propagation} API on the classpath?
	 *
	 * @return true if context-propagation is available at runtime, false otherwise
	 */
	static boolean isContextPropagationAvailable() {
		return isContextPropagationAvailable;
	}

	/**
	 * Create a support function that takes a snapshot of thread locals and merges them with the
	 * provided {@link Context}, resulting in a new {@link Context} which includes entries
	 * captured from threadLocals by the Context-Propagation API.
	 *
	 * @return the {@link Context} augmented with captured entries
	 */
	public static Function<Context, Context> contextCapture() {
		if (!isContextPropagationAvailable) {
			return NO_OP;
		}
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
	 *
	 * @param captureKeyPredicate a {@link Predicate} used on keys to determine if each entry
	 * should be injected into the new {@link Context}
	 * @return a {@link Function} augmenting {@link Context} with captured entries
	 */
	public static Function<Context, Context> contextCapture(Predicate<Object> captureKeyPredicate) {
		if (!isContextPropagationAvailable) {
			return NO_OP;
		}
		return new ContextCaptureFunction(captureKeyPredicate, null);
	}

	public static <T, R> BiConsumer<T, SynchronousSink<R>> contextRestoreForHandle(BiConsumer<T, SynchronousSink<R>> handler, CoreSubscriber<? super R> actual) {
		if (!ContextPropagation.isContextPropagationAvailable() || !actual.currentContext().hasKey(ContextPropagation.CAPTURED_CONTEXT_MARKER)) {
			return handler;
		}
		return (t, s) -> {
			try (ContextSnapshot.Scope ignored = ContextSnapshot.setThreadLocalsFrom(actual.currentContext())) {
				handler.accept(t, s);
			}
		};
	}

	public static <T> SignalListener<T> contextRestoringSignalListener(final SignalListener<T> original,
																	   CoreSubscriber<? super T> actual) {
		if (!ContextPropagation.isContextPropagationAvailable() || !actual.currentContext().hasKey(ContextPropagation.CAPTURED_CONTEXT_MARKER)) {
			return original;
		}
		return new ContextRestoreSignalListener<T>(original, actual.currentContext(), ContextRegistry.getInstance());
	}

	//the Function indirection allows tests to directly assert code in this class rather than static methods
	static final class ContextCaptureFunction implements Function<Context, Context> {

		final Predicate<Object> capturePredicate;
		final ContextRegistry registry;

		ContextCaptureFunction(Predicate<Object> capturePredicate, @Nullable ContextRegistry registry) {
			this.capturePredicate = capturePredicate;
			this.registry = registry != null ? registry : ContextRegistry.getInstance();
		}

		@Override
		public Context apply(Context target) {
			return ContextSnapshot.captureAllUsing(capturePredicate, this.registry)
				.updateContext(target)
				.put(CAPTURED_CONTEXT_MARKER, true);
		}
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

		@Override
		public void doFirst() throws Throwable {
			try (ContextSnapshot.Scope ignored = ContextSnapshot.setThreadLocalsFrom(this.context, this.registry)) {
				original.doFirst();
			}
		}

		@Override
		public void doFinally(SignalType terminationType) throws Throwable {
			try (ContextSnapshot.Scope ignored = ContextSnapshot.setThreadLocalsFrom(this.context, this.registry)) {
				original.doFinally(terminationType);
			}
		}

		@Override
		public void doOnSubscription() throws Throwable {
			try (ContextSnapshot.Scope ignored = ContextSnapshot.setThreadLocalsFrom(this.context)) {
				original.doOnSubscription();
			}
		}

		@Override
		public void doOnFusion(int negotiatedFusion) throws Throwable {
			try (ContextSnapshot.Scope ignored = ContextSnapshot.setThreadLocalsFrom(this.context, this.registry)) {
				original.doOnFusion(negotiatedFusion);
			}
		}

		@Override
		public void doOnRequest(long requested) throws Throwable {
			try (ContextSnapshot.Scope ignored = ContextSnapshot.setThreadLocalsFrom(this.context, this.registry)) {
				original.doOnRequest(requested);
			}
		}

		@Override
		public void doOnCancel() throws Throwable {
			try (ContextSnapshot.Scope ignored = ContextSnapshot.setThreadLocalsFrom(this.context, this.registry)) {
				original.doOnCancel();
			}
		}

		@Override
		public void doOnNext(T value) throws Throwable {
			try (ContextSnapshot.Scope ignored = ContextSnapshot.setThreadLocalsFrom(this.context, this.registry)) {
				original.doOnNext(value);
			}
		}

		@Override
		public void doOnComplete() throws Throwable {
			try (ContextSnapshot.Scope ignored = ContextSnapshot.setThreadLocalsFrom(this.context, this.registry)) {
				original.doOnComplete();
			}
		}

		@Override
		public void doOnError(Throwable error) throws Throwable {
			try (ContextSnapshot.Scope ignored = ContextSnapshot.setThreadLocalsFrom(this.context, this.registry)) {
				original.doOnError(error);
			}
		}

		@Override
		public void doAfterComplete() throws Throwable {
			try (ContextSnapshot.Scope ignored = ContextSnapshot.setThreadLocalsFrom(this.context, this.registry)) {
				original.doAfterComplete();
			}
		}

		@Override
		public void doAfterError(Throwable error) throws Throwable {
			try (ContextSnapshot.Scope ignored = ContextSnapshot.setThreadLocalsFrom(this.context, this.registry)) {
				original.doAfterError(error);
			}
		}

		@Override
		public void doOnMalformedOnNext(T value) throws Throwable {
			try (ContextSnapshot.Scope ignored = ContextSnapshot.setThreadLocalsFrom(this.context, this.registry)) {
				original.doOnMalformedOnNext(value);
			}
		}

		@Override
		public void doOnMalformedOnError(Throwable error) throws Throwable {
			try (ContextSnapshot.Scope ignored = ContextSnapshot.setThreadLocalsFrom(this.context, this.registry)) {
				original.doOnMalformedOnError(error);
			}
		}

		@Override
		public void doOnMalformedOnComplete() throws Throwable {
			try (ContextSnapshot.Scope ignored = ContextSnapshot.setThreadLocalsFrom(this.context, this.registry)) {
				original.doOnMalformedOnComplete();
			}
		}

		@Override
		public void handleListenerError(Throwable listenerError) {
			try (ContextSnapshot.Scope ignored = ContextSnapshot.setThreadLocalsFrom(this.context, this.registry)) {
				original.handleListenerError(listenerError);
			}
		}

		@Override
		public Context addToContext(Context originalContext) {
			try (ContextSnapshot.Scope ignored = ContextSnapshot.setThreadLocalsFrom(this.context, this.registry)) {
				return original.addToContext(originalContext);
			}
		}
	}
}
