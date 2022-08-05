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

import java.util.function.Function;
import java.util.function.Predicate;

import io.micrometer.context.ContextRegistry;
import io.micrometer.context.ContextSnapshot;

import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Utility private class to detect if Context-Propagation is on the classpath and to offer
 * ContextSnapshot support to {@link Flux} and {@link Mono}.
 *
 * @author Simon Baslé
 */
final class ContextPropagation {

	static final boolean isContextPropagationAvailable;

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
			return ContextSnapshot.captureUsing(this.registry, capturePredicate).updateContext(target);
		}
	}

}
