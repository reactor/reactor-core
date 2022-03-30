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
import java.util.function.Consumer;
import java.util.function.Function;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * @author Simon Baslé
 */
public class ContextPropagationUtils {

	public static final class ThreadLocalHelper {

		public final List<String>        capturedState;
		public final ThreadLocal<String> threadLocal;

		public ThreadLocalHelper() {
			this.capturedState = new CopyOnWriteArrayList<>();
			threadLocal = ThreadLocal.withInitial(() -> "none");

			Schedulers.onScheduleContextualHook("threadLocalHelper", (r, c) -> {
				if (c.isEmpty()) return r;
				final String fromContext = c.getOrDefault("threadLocalHelper", "notFound");
				return () -> {
					threadLocal.set(fromContext);
					r.run();
					threadLocal.remove();
				};
			});
		}

		public <R> String map(R input) {
			return "" + input + this.threadLocal.get();
		}

		public <R> String mapAndConsume(R input) {
			this.capturedState.add(input + "->" + this.threadLocal.get());
			return "" + input + this.threadLocal.get();
		}

		public <R> void consume(R input) {
			this.capturedState.add(input + "->" + this.threadLocal.get());
		}

		public <R> void ignore(R input) {
			this.capturedState.add(this.threadLocal.get());
		}

		public void run() {
			this.capturedState.add(threadLocal.get());
		}

		public Runnable runTagged(String tag) {
			return () -> this.capturedState.add(tag + "->" + threadLocal.get());
		}

		public <R> Mono<R> withContext(Mono<R> source) {
			return source.contextWrite(CONTEXT);
		}

		public <R> Flux<R> withContext(Flux<R> source) {
			return source.contextWrite(CONTEXT);
		}

		public <R> StepVerifier.FirstStep<R> stepVerifier(Publisher<R> underTest) {
			return StepVerifier.create(underTest,
				StepVerifierOptions.create()
					.withInitialContext(Context.of(CONTEXT)));
		}

		public <R> Function<Publisher<R>, StepVerifier.FirstStep<R>> stepVerifierWithOptions(
			Consumer<StepVerifierOptions> optionsModifier) {
			StepVerifierOptions options = StepVerifierOptions.create();
			optionsModifier.accept(options);
			options.withInitialContext(Context.of(CONTEXT));

			return underTest -> StepVerifier.create(underTest, options);
		}

		public ListAssert<String> assertThatCapturedState() {
			return Assertions.assertThat(this.capturedState);
		}

		public static final ContextView CONTEXT = Context.of("threadLocalHelper", "customized");
	}
}
