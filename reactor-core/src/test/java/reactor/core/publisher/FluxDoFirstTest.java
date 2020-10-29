/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

public class FluxDoFirstTest {

	@Test
	public void rejectsNullRunnable() {
		Flux<Integer> source = Flux.empty();
		//noinspection ConstantConditions
		assertThatNullPointerException()
				.isThrownBy(() -> source.doFirst(null))
				.withMessage("onFirst");
	}

	@Test
	public void orderIsReversed_NoFusion() {
		List<String> order = new ArrayList<>();

		@SuppressWarnings("divzero")
		Function<Integer, Integer> divZero = i -> i / 0;

		StepVerifier.create(
				Flux.just(1)
				    .map(divZero)
				    .hide()
				    .doFirst(() -> order.add("one"))
				    .doFirst(() -> order.add("two"))
				    .doFirst(() -> order.add("three"))
		)
		            .expectNoFusionSupport()
		            .verifyError(ArithmeticException.class);

		assertThat(order).containsExactly("three", "two", "one");
	}

	@Test
	public void orderIsReversed_Fused() {
		List<String> order = new ArrayList<>();

		@SuppressWarnings("divzero")
		Function<Integer, Integer> divZero = i -> i / 0;

		StepVerifier.create(
				Flux.just(1)
				    .map(divZero)
				    .doFirst(() -> order.add("one"))
				    .doFirst(() -> order.add("two"))
				    .doFirst(() -> order.add("three"))
		)
		            .expectFusion()
		            .verifyError(ArithmeticException.class);

		assertThat(order).containsExactly("three", "two", "one");
	}

	@Test
	public void mixedWithOnSubscribe_NoFusion() {
		List<String> order = new ArrayList<>();

		StepVerifier.create(
				Flux.just(1, 2)
				    .hide()
				    .doOnNext(i -> order.add("doOnNext" + i))
				    .doFirst(() -> order.add("doFirst1"))
				    .doOnSubscribe(sub -> order.add("doOnSubscribe1"))
				    .doFirst(() -> order.add("doFirst2"))
				    .doOnSubscribe(sub -> order.add("doOnSubscribe2"))
		)
		            .expectNoFusionSupport()
		            .expectNext(1, 2)
		            .verifyComplete();

		assertThat(order).containsExactly("doFirst2", "doFirst1", "doOnSubscribe1",
				"doOnSubscribe2", "doOnNext1", "doOnNext2");
	}

	@Test
	public void mixedWithOnSubscribe_Fused() {
		List<String> order = new ArrayList<>();

		StepVerifier.create(
		Flux.just(1, 2)
		    .doOnNext(i -> order.add("doOnNext" + i))
		    .doFirst(() -> order.add("doFirst1"))
		    .doOnSubscribe(sub -> order.add("doOnSubscribe1"))
		    .doFirst(() -> order.add("doFirst2"))
		    .doOnSubscribe(sub -> order.add("doOnSubscribe2"))
		)
		            .expectFusion()
		            .expectNext(1, 2)
		            .verifyComplete();

		assertThat(order).containsExactly("doFirst2", "doFirst1", "doOnSubscribe1",
				"doOnSubscribe2", "doOnNext1", "doOnNext2");
	}

	@Test
	public void runnableFailure_NotFuseable() {
		Flux<Integer> test = Flux.just(1, 2)
		                         .hide()
		                         .doFirst(() -> {
			                         throw new IllegalStateException("expected");
		                         });

		assertThat(test).as("doFirst not fuseable").isNotInstanceOf(Fuseable.class);
		StepVerifier.create(test)
		            .expectSubscription()
		            .verifyErrorMessage("expected");
	}

	@Test
	public void runnableFailure_Fuseable() {
		Flux<Integer> test = Flux.just(1, 2)
		                         .doFirst(() -> {
			                         throw new IllegalStateException("expected");
		                         });

		assertThat(test).as("doFirst is fuseable").isInstanceOf(Fuseable.class);
		StepVerifier.create(test)
		            .expectSubscription()
		            .verifyErrorMessage("expected");
	}

	@Test
	public void performsDirectSubscriberToSource_NoFusion() {
		AtomicReference<Subscription> subRef = new AtomicReference<>();
		Flux<Integer> test = Flux.just(1, 2)
		                         .hide()
		                         .doFirst(() -> {})
		                         .doOnSubscribe(subRef::set);
		StepVerifier.create(test).expectNextCount(2).verifyComplete();

		assertThat(subRef.get().getClass()).isEqualTo(FluxHide.HideSubscriber.class);
	}

	@Test
	public void performsDirectSubscriberToSource_Fused() {
		AtomicReference<Subscription> subRef = new AtomicReference<>();
		Flux<Integer> test = Flux.just(1, 2)
		                         .map(Function.identity())
		                         .doFirst(() -> {})
		                         .doOnSubscribe(subRef::set);
		StepVerifier.create(test).expectNextCount(2).verifyComplete();

		assertThat(subRef.get().getClass()).isEqualTo(FluxMapFuseable.MapFuseableSubscriber.class);
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxDoFirst<Integer> test = new FluxDoFirst<>(parent, () -> {});

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanOperatorFuseable(){
		Flux<Integer> parent = Flux.just(1);
		FluxDoFirstFuseable<Integer> test = new FluxDoFirstFuseable<>(parent, () -> {});

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
