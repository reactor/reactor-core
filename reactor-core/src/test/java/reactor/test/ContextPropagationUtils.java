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

package reactor.test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import io.micrometer.contextpropagation.ContextContainer;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * Test support class to help crafting tests around {@link Schedulers#onScheduleContextualHook(String, BiFunction)}.
 * @author Simon Baslé
 */
public class ContextPropagationUtils {

	/**
	 * Stores the current test's thread local if the test uses a {@link ThreadLocalHelper}. Will be reset to null by
	 * {@link reactor.ReactorTestExecutionListener} after each test, so we're sure that the TL is not polluted by other tests.
	 */
	public static final AtomicReference<ThreadLocal<String>> THREAD_LOCAL_REF = new AtomicReference<>(null);

	/**
	 * Test helper class to install a {@link Schedulers#onScheduleContextualHook(String, BiFunction) hook} that restores
	 * values from {@link ContextView} into a {@link ThreadLocal} + decorator methods to use in {@link Consumer}, {@link Runnable}
	 * and {@link Function}-based operators which read their visible {@link ThreadLocal} value for assertions.
	 * <p>
	 * The hook tries to read the "threadLocalHelper" key from {@link ContextView} and put the associated value in the {@link ThreadLocal},
	 * or "notFound". The {@link ThreadLocal} defaults to "none" if no hook has written to it.
	 * <p>
	 * In order to verify context is propagated to interesting operators, typical usage in a chain of operators under test is as follows:
	 * <ul>
	 *     <li>
	 *         add relevant operator to apply threadLocal-capturing logic based on methods like {@link #map(Object)} or {@link #consume(Object)}
	 *     </li>
	 *     <li>
	 *         ensure the relevant key is added to the chain's context via {@link #withContext(Flux)} or directly via creation
	 *         of a {@link #stepVerifier(Publisher) StepVerifier}.
	 *     </li>
	 *     <li>
	 *         assert the signals via the {@link #stepVerifier(Publisher) StepVerifier} or by subscribing manually.
	 *     </li>
	 *     <li>
	 *         optionally also assert side effect captures via {@link #assertThatCapturedState()}
	 *     </li>
	 * </ul>
	 */
	public static final class ThreadLocalHelper {

		public final List<String>        capturedState;
		public final ThreadLocal<String> threadLocal;

		/**
		 * Constructs the helper and installs a {@link ThreadLocal}-restoring hook.
		 */
		public ThreadLocalHelper() {
			this.capturedState = new CopyOnWriteArrayList<>();
			this.threadLocal = ThreadLocal.withInitial(() -> "none");
			THREAD_LOCAL_REF.set(threadLocal);

			Schedulers.onScheduleContextualHook("threadLocalHelper", (r, c) -> {
				if (c.isEmpty()) return r;

				ContextContainer container = ContextContainer.create();
				container.captureContext(c);
				container.captureThreadLocalValues();

				return () -> container.tryScoped(r);
			});
		}

		/**
		 * A transformation method that appends the {@link ThreadLocal} value to the input.
		 * Method reference can be used as a {@link Function} for operators like map.
		 *
		 * @param input the input value
		 * @param <R> the type of input
		 * @return a {@link String} concatenation of the input + the {@link ThreadLocal} value
		 */
		public <R> String map(R input) {
			return "" + input + this.threadLocal.get();
		}

		/**
		 * A transformation method that appends the {@link ThreadLocal} value to the input.
		 * Method reference can be used as a {@link Function} for operators like map.
		 * Unlike {@link #map(Object)}, this one also captures the input + threadlocal pair (with
		 * a {@code ->} separator) to the list for later assertion via {@link #assertThatCapturedState()}.
		 *
		 * @param input the input value
		 * @param <R> the type of input
		 * @return a {@link String} concatenation of the input + the {@link ThreadLocal} value
		 */
		public <R> String mapAndConsume(R input) {
			this.capturedState.add(input + "->" + this.threadLocal.get());
			return "" + input + this.threadLocal.get();
		}

		/**
		 * Consume the input by putting the input + threadlocal pair (with a {@code ->} separator)
		 * into the captured state list for later assertion via {@link #assertThatCapturedState()}.
		 * Method reference can be used as a {@link Consumer} for operators like doOnNext.
		 *
		 * @param input the input value
		 * @param <R> the type of input
		 */
		public <R> void consume(R input) {
			this.capturedState.add(input + "->" + this.threadLocal.get());
		}

		/**
		 * Ignore the input but put the threadlocal value into the captured state list
		 * for later assertion via {@link #assertThatCapturedState()}.
		 * Method reference can be used as a {@link Consumer} for operators like doOnNext.
		 *
		 * @param input the input value
		 * @param <R> the type of input
		 */
		public <R> void ignore(R input) {
			this.capturedState.add(this.threadLocal.get());
		}

		/**
		 * Simply put the threadlocal value into the captured state list for later assertion
		 * via {@link #assertThatCapturedState()}.
		 * Method reference can be used as a {@link Runnable} for operators like doOnComplete.
		 */
		public void run() {
			this.capturedState.add(this.threadLocal.get());
		}

		/**
		 * Create a {@link Runnable} that concatenates the provided {@code tag}, a {@code ->} separator
		 * and the {@link ThreadLocal} value and adds the resulting string into the captured state list,
		 * for later assertion via {@link #assertThatCapturedState()}.
		 * Resulting {@link Runnable} can be used for operators like doOnComplete.
		 *
		 * @param tag the name of the event at which the threadLocal is being captured
		 * @return a {@link Runnable} that captures the tag + threadLocal value
		 */
		public Runnable runTagged(String tag) {
			return () -> this.capturedState.add(tag + "->" + this.threadLocal.get());
		}

		/**
		 * Add the {@code ["threadLocalHelper", "customized"]} internal key-value pair to the context
		 * of a given {@link Mono}. The value is the one extracted and put in threadLocal by
		 * the hook installed at construction.
		 *
		 * @param source the source {@link Mono}
		 * @param <R> the type of data in the Mono
		 * @return the Mono with {@code ["threadLocalHelper", "customized"]} pair in context
		 */
		public <R> Mono<R> withContext(Mono<R> source) {
			return source.contextWrite(CONTEXT);
		}

		/**
		 * Add the {@code ["threadLocalHelper", "customized"]} internal key-value pair to the context
		 * of a given {@link Flux}. The value is the one extracted and put in threadLocal by
		 * the hook installed at construction.
		 *
		 * @param source the source {@link Flux}
		 * @param <R> the type of data in the Flux
		 * @return the Flux with {@code ["threadLocalHelper", "customized"]} pair in context
		 */
		public <R> Flux<R> withContext(Flux<R> source) {
			return source.contextWrite(CONTEXT);
		}

		/**
		 * Create a {@link StepVerifier} out of the provided {@link Publisher}, ensuring the
		 * {@code ["threadLocalHelper", "customized"]} internal key-value pair is added to the context.
		 * <p>
		 * The Method Reference can be used directly with the {@link Flux#as(Function) as} operator.
		 * <p>
		 * The value associated with {@code threadLocalHelper} key is the one extracted and put in threadLocal
		 * by the hook installed at construction.
		 *
		 * @param <R> the type of data in the {@link Publisher} under test
		 * @return a {@link StepVerifier} to test the {@link Publisher}, with relevant context
		 */
		public <R> StepVerifier.FirstStep<R> stepVerifier(Publisher<R> underTest) {
			return StepVerifier.create(underTest,
				StepVerifierOptions.create()
					.withInitialContext(Context.of(CONTEXT)));
		}

		/**
		 * Return a {@link StepVerifier}-creating {@link Function} while allowing customization of
		 * {@link StepVerifierOptions} (as provided to the required {@link Consumer}). On top of
		 * any modification made to options, the resulting {@link Function} will ensure the
		 * {@code ["threadLocalHelper", "customized"]} internal key-value pair is added to the context.
		 * <p>
		 * The resulting function can  be used with the {@link Flux#as(Function) as} operator.
		 * <p>
		 * The value associated with {@code threadLocalHelper} key is the one extracted and put in threadLocal
		 * by the hook installed at construction.
		 *
		 * @param <R> the type of data in the {@link Publisher} under test
		 * @return a {@link StepVerifier} to test the {@link Publisher}, with relevant context
		 */
		public <R> Function<Publisher<R>, StepVerifier.FirstStep<R>> stepVerifierWithOptions(
			Consumer<StepVerifierOptions> optionsModifier) {
			StepVerifierOptions options = StepVerifierOptions.create();
			optionsModifier.accept(options);
			Context context = options.getInitialContext() == null ? Context.of(CONTEXT) : options.getInitialContext().putAll(CONTEXT);
			options.withInitialContext(context);

			return underTest -> StepVerifier.create(underTest, options);
		}

		/**
		 * Exposes a {@link ListAssert} on the list of captured key->threadLocal pairs as
		 * stored by {@link #mapAndConsume(Object)}, {@link #consume(Object)}, {@link #ignore(Object)},
		 * {@link #run()} and {@link #runTagged(String)}.
		 *
		 * @return the {@link ListAssert} allowing further assertion of the list of captured states
		 */
		public ListAssert<String> assertThatCapturedState() {
			return Assertions.assertThat(this.capturedState);
		}

		/**
		 * The {@link ContextView} used by the hook and context-writing methods.
		 */
		public static final ContextView CONTEXT = Context.of(ContextPropagationUtilsPropagator.IN_CONTEXT_KEY, "customized");
	}
}
