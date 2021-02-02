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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class FluxRepeatWhenTest {

	@Test
	public void whenFactoryNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.never()
					.repeatWhen(null);
		});
	}

	@Test
	public void coldRepeater() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.just(1)
		    .repeatWhen(v -> Flux.range(1, 10))
		    .subscribe(ts);

		ts.assertValues(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void cancelsOther() {
		AtomicBoolean cancelled = new AtomicBoolean();
		Flux<Integer> when = Flux.range(1, 10)
		                         .doOnCancel(() -> cancelled.set(true));

		StepVerifier.create(Flux.just(1).repeatWhen(other -> when))
		            .thenCancel()
		            .verify();

		assertThat(cancelled.get()).isTrue();
	}

	@Test
	public void cancelTwiceCancelsOtherOnce() {
		AtomicInteger cancelled = new AtomicInteger();
		Flux<Integer> when = Flux.range(1, 10)
		                         .doOnCancel(cancelled::incrementAndGet);

		Flux.just(1)
		    .repeatWhen(other -> when)
		    .subscribe(new BaseSubscriber<Integer>() {
			    @Override
			    protected void hookOnSubscribe(Subscription subscription) {
				    subscription.request(1);
				    subscription.cancel();
				    subscription.cancel();
			    }
		    });

		assertThat(cancelled).hasValue(1);
	}

	@Test
	public void directOtherErrorPreventsSubscribe() {
		AtomicBoolean sourceSubscribed = new AtomicBoolean();
		AtomicBoolean sourceCancelled = new AtomicBoolean();
		Flux<Integer> source = Flux.just(1)
		                           .doOnSubscribe(sub -> sourceSubscribed.set(true))
		                           .doOnCancel(() -> sourceCancelled.set(true));

		Flux<Integer> repeat = source.repeatWhen(other -> Mono.error(new IllegalStateException("boom")));

		StepVerifier.create(repeat)
		            .expectSubscription()
		            .verifyErrorMessage("boom");

		assertThat(sourceSubscribed.get()).isFalse();
		assertThat(sourceCancelled.get()).isFalse();
	}

	@Test
	public void lateOtherErrorCancelsSource() {
		AtomicBoolean sourceSubscribed = new AtomicBoolean();
		AtomicBoolean sourceCancelled = new AtomicBoolean();
		AtomicInteger count = new AtomicInteger();
		Flux<Integer> source = Flux.just(1)
		                           .doOnSubscribe(sub -> sourceSubscribed.set(true))
		                           .doOnCancel(() -> sourceCancelled.set(true));


		Flux<Integer> repeat = source.repeatWhen(other -> other.flatMap(l ->
				count.getAndIncrement() == 0 ? Mono.just(l) : Mono.error(new IllegalStateException("boom"))));

		StepVerifier.create(repeat)
		            .expectSubscription()
		            .expectNext(1)
		            .expectNext(1)
		            .verifyErrorMessage("boom");

		assertThat(sourceSubscribed.get()).isTrue();
		assertThat(sourceCancelled.get()).isTrue();
	}

	@Test
	public void directOtherEmptyPreventsSubscribe() {
		AtomicBoolean sourceSubscribed = new AtomicBoolean();
		AtomicBoolean sourceCancelled = new AtomicBoolean();
		Flux<Integer> source = Flux.just(1)
		                           .doOnSubscribe(sub -> sourceSubscribed.set(true))
		                           .doOnCancel(() -> sourceCancelled.set(true));

		Flux<Integer> repeat = source.repeatWhen(other -> Flux.empty());

		StepVerifier.create(repeat)
		            .expectSubscription()
		            .verifyComplete();

		assertThat(sourceSubscribed.get()).isFalse();
		assertThat(sourceCancelled.get()).isFalse();
	}

	@Test
	public void lateOtherEmptyCancelsSource() {
		AtomicBoolean sourceSubscribed = new AtomicBoolean();
		AtomicBoolean sourceCancelled = new AtomicBoolean();
		Flux<Integer> source = Flux.just(1)
		                           .doOnSubscribe(sub -> sourceSubscribed.set(true))
		                           .doOnCancel(() -> sourceCancelled.set(true));

		Flux<Integer> repeat = source.repeatWhen(other -> other.take(1));

		StepVerifier.create(repeat)
		            .expectSubscription()
		            .expectNext(1) //original
		            .expectNext(1) //repeat
		            .verifyComplete(); //repeat terminated

		assertThat(sourceSubscribed.get()).isTrue();
		assertThat(sourceCancelled.get()).isTrue();
	}

	@Test
	public void coldRepeaterBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 2)
		    .repeatWhen(v -> Flux.range(1, 5))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2, 1)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void coldEmpty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 2)
		    .repeatWhen(v -> Flux.empty())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void coldError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 2)
		    .repeatWhen(v -> Flux.error(new RuntimeException("forced " + "failure")))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void whenFactoryThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .repeatWhen(v -> {
			    throw new RuntimeException("forced failure");
		    })
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

	}

	@Test
	public void whenFactoryReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		new FluxRepeatWhen<>(Flux.range(1, 2), v -> null).subscribe(ts);

		ts.assertNoValues()
		  .assertError(NullPointerException.class)
		  .assertNotComplete();

	}

	@Test
	public void repeaterErrorsInResponse() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .repeatWhen(v -> v.map(a -> {
			    throw new RuntimeException("forced failure");
		    }))
		    .subscribe(ts);

		ts.assertValues(1, 2)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

	}

	@Test
	public void repeatAlways() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 2)
		    .repeatWhen(v -> v)
		    .subscribe(ts);

		ts.request(8);

		ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2)
		  .assertNoError()
		  .assertNotComplete();
	}

	@Test
	public void repeatAlwaysScalar() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		AtomicInteger count = new AtomicInteger();

		Flux.just(1)
		    .map(d -> count.incrementAndGet())
		    .repeatWhen(v -> v)
		    .subscribe(ts);

		ts.request(8);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8)
		  .assertNoError()
		  .assertNotComplete();
	}

	@Test
	public void repeatWithVolumeCondition() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 2)
		    .repeatWhen(v -> v.takeWhile(n -> n > 0))
		    .subscribe(ts);

		ts.request(8);

		ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2)
		  .assertNoError()
		  .assertNotComplete();
	}

	@Test
	public void exponentialRepeat() {
		StepVerifier.withVirtualTime(this::exponentialRepeatScenario1)
		            .expectNext(1)
		            .thenAwait(Duration.ofSeconds(1))
		            .expectNext(2)
		            .thenAwait(Duration.ofSeconds(2))
		            .expectNext(3)
		            .thenAwait(Duration.ofSeconds(3))
		            .expectNext(4)
		            .expectComplete()
		            .verify();
	}

	Flux<Integer> exponentialRepeatScenario1() {
		AtomicInteger i = new AtomicInteger();
		return Mono.fromCallable(i::incrementAndGet)
		           .repeatWhen(repeat -> repeat.zipWith(Flux.range(1, 3), (t1, t2) -> t2)
		                                       .flatMap(time -> Mono.delay(Duration.ofSeconds(
				                                       time))));
	}

	@Test
	public void exponentialRepeat2() {
		StepVerifier.withVirtualTime(this::exponentialRepeatScenario2)
		            .thenAwait(Duration.ofSeconds(6))
		            .expectNext("hey")
		            .expectComplete()
		            .verify();
	}

	Flux<String> exponentialRepeatScenario2() {
		AtomicInteger i = new AtomicInteger();
		return Mono.<String>create(s -> {
			if (i.incrementAndGet() == 4) {
				s.success("hey");
			}
			else {
				s.success();
			}
		}).repeatWhen(repeat -> repeat.zipWith(Flux.range(1, 3), (t1, t2) -> t2)
		                              .flatMap(time -> Mono.delay(Duration.ofSeconds(time))));
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxRepeatWhen<Integer> test = new FluxRepeatWhen<>(parent, c -> c.take(3));

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    public void scanMainSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxRepeatWhen.RepeatWhenMainSubscriber<Integer> test =
        		new FluxRepeatWhen.RepeatWhenMainSubscriber<>(actual, null, Flux.empty());
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
        test.requested = 35;
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35L);

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
    public void scanOtherSubscriber() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxRepeatWhen.RepeatWhenMainSubscriber<Integer> main =
        		new FluxRepeatWhen.RepeatWhenMainSubscriber<>(actual, null, Flux.empty());
        FluxRepeatWhen.RepeatWhenOtherSubscriber test = new FluxRepeatWhen.RepeatWhenOtherSubscriber();
        test.main = main;

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(main.otherArbiter);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }

	@Test
	public void inners() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		Sinks.Many<Long> signaller = Sinks.unsafe().many().multicast().directBestEffort();
		Flux<Integer> when = Flux.empty();
		FluxRepeatWhen.RepeatWhenMainSubscriber<Integer> main = new FluxRepeatWhen.RepeatWhenMainSubscriber<>(actual, signaller, when);

		List<Scannable> inners = main.inners().collect(Collectors.toList());

		assertThat(inners).containsExactly((Scannable) signaller, main.otherArbiter);
	}

	@Test
	public void repeatWhenContextTrigger_MergesOriginalContext() {
		final int REPEAT_COUNT = 3;
		List<String> repeats = Collections.synchronizedList(new ArrayList<>(4));
		List<ContextView> contexts = Collections.synchronizedList(new ArrayList<>(4));

		Flux<String> retryWithContext =
				Flux.just("A", "B")
				    .doOnEach(sig -> {
				    	if (sig.isOnComplete()) {
						    ContextView ctx = sig.getContextView();
						    contexts.add(ctx);
						    repeats.add("emitted " + ctx.get("emitted") + " elements this attempt, " + ctx.get("repeatsLeft") + " repeats left");
					    }
				    })
				    .repeatWhen(emittedEachAttempt -> emittedEachAttempt.handle((lastEmitted, sink) -> {
					    Context ctx = sink.currentContext();
					    int rl = ctx.getOrDefault("repeatsLeft", 0);
					    if (rl > 0) {
					        sink.next(Context.of(
								    "repeatsLeft", rl - 1,
								    "emitted", lastEmitted
						    ));
					    }
					    else {
					        sink.error(new IllegalStateException("repeats exhausted"));
					    }
				    }))
				    .contextWrite(Context.of("repeatsLeft", REPEAT_COUNT, "emitted", 0))
				    .contextWrite(Context.of("thirdPartyContext", "present"));

		StepVerifier.create(retryWithContext)
		            .expectNext("A", "B")
		            .expectNext("A", "B")
		            .expectNext("A", "B")
		            .expectNext("A", "B")
		            .expectErrorMessage("repeats exhausted")
		            .verify(Duration.ofSeconds(1));

		assertThat(repeats).containsExactly(
				"emitted 0 elements this attempt, 3 repeats left",
				"emitted 2 elements this attempt, 2 repeats left",
				"emitted 2 elements this attempt, 1 repeats left",
				"emitted 2 elements this attempt, 0 repeats left");

		assertThat(contexts).allMatch(ctx -> ctx.hasKey("thirdPartyContext"));
	}

	@Test
	void gh2579() {
		 for (int i = 0; i < 1_000; i++) {
			AtomicInteger sourceHelper = new AtomicInteger();
			Flux.just("hello")
					.filter(m -> sourceHelper.getAndIncrement() >= 9)
					.repeatWhen(it -> it.delayElements(Duration.ofNanos(1)))
					.blockFirst(Duration.ofSeconds(1));
		 }
	}
}
