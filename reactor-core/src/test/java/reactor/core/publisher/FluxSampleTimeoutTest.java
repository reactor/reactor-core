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

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.FluxSampleTimeout.SampleTimeoutOther;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxSampleTimeoutTest {

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp3 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux()
		   .sampleTimeout(v -> v == 1 ? sp2.asFlux() : sp3.asFlux())
		   .subscribe(ts);

		sp1.emitNext(1, FAIL_FAST);
		sp2.emitNext(1, FAIL_FAST);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		sp1.emitNext(2, FAIL_FAST);
		sp1.emitNext(3, FAIL_FAST);
		sp1.emitNext(4, FAIL_FAST);

		sp3.emitNext(2, FAIL_FAST);

		ts.assertValues(1, 4)
		  .assertNoError()
		  .assertNotComplete();

		sp1.emitNext(5, FAIL_FAST);
		sp1.emitComplete(FAIL_FAST);

		ts.assertValues(1, 4, 5)
		  .assertNoError()
		  .assertComplete();

		assertThat(sp1.currentSubscriberCount()).as("sp1 has subscriber").isZero();
		assertThat(sp2.currentSubscriberCount()).as("sp2 has subscriber").isZero();
		assertThat(sp3.currentSubscriberCount()).as("sp3 has subscriber").isZero();
	}

	@Test
	public void mainError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux()
		   .sampleTimeout(v -> sp2.asFlux())
		   .subscribe(ts);

		sp1.emitNext(1, FAIL_FAST);
		sp1.emitError(new RuntimeException("forced failure"), FAIL_FAST);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		assertThat(sp1.currentSubscriberCount()).as("sp1 has subscriber").isZero();
		assertThat(sp2.currentSubscriberCount()).as("sp2 has subscriber").isZero();
	}

	@Test
	public void throttlerError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux()
		   .sampleTimeout(v -> sp2.asFlux())
		   .subscribe(ts);

		sp1.emitNext(1, FAIL_FAST);
		sp2.emitError(new RuntimeException("forced failure"), FAIL_FAST);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		assertThat(sp1.currentSubscriberCount()).as("sp1 has subscriber").isZero();
		assertThat(sp2.currentSubscriberCount()).as("sp2 has subscriber").isZero();
	}

	@Test
	public void throttlerReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux()
		   .sampleTimeout(v -> null)
		   .subscribe(ts);

		sp1.emitNext(1, FAIL_FAST);

		ts.assertNoValues()
		  .assertError(NullPointerException.class)
		  .assertNotComplete();

		assertThat(sp1.currentSubscriberCount()).as("sp1 has subscriber").isZero();
	}

	@Test
	public void sampleIncludesLastItem() {
		StepVerifier.withVirtualTime(() ->
				Flux.concat(
						Flux.range(1, 5),
						Mono.delay(Duration.ofMillis(260)).ignoreElement().map(Long::intValue),
						Flux.just(80, 90, 100)
				).hide()
						.sampleTimeout(i -> Mono.delay(Duration.ofMillis(250))))
				.thenAwait(Duration.ofMillis(500))
				.expectNext(5)
				.expectNext(100)
				.verifyComplete();
	}

	@Test
	public void sourceTerminatesBeforeSamplingEmitsLast() {
		Flux<Integer> source = Flux.just(1, 2).hide();

		Duration duration = StepVerifier.create(source
				.sampleTimeout(i -> Mono.delay(Duration.ofMillis(250))))
		                                .expectNext(2)
		                                .verifyComplete();

		//sanity check on the sequence duration
		assertThat(duration.toMillis()).isLessThan(250);
	}

	@Test
	public void sourceErrorsBeforeSamplingNoEmission() {
		Flux<Integer> source = Flux.just(1, 2).concatWith(Mono.error(new IllegalStateException("boom")));

		Duration duration = StepVerifier.create(source
				.sampleTimeout(i -> Mono.delay(Duration.ofMillis(250))))
		                                .verifyErrorMessage("boom");

		//sanity check on the sequence duration
		assertThat(duration.toMillis()).isLessThan(250);
	}

	Flux<Integer> scenario_sampleTimeoutTime(){
		return Flux.range(1, 10)
		           .delayElements(Duration.ofMillis(300))
		           .sampleTimeout(d -> Mono.delay(Duration.ofMillis(100*d)), 1);
	}

	@Test
	public void sampleTimeoutTime(){
		StepVerifier.withVirtualTime(this::scenario_sampleTimeoutTime)
		            .thenAwait(Duration.ofSeconds(10))
		            .expectNext(1, 2, 3, 10)
		            .verifyComplete();
	}
	Flux<Integer> scenario_sampleTimeoutTime2(){
		return Flux.range(1, 10)
		           .delayElements(Duration.ofMillis(300))
		           .sampleTimeout(d -> Mono.delay(Duration.ofMillis(100*d)), Integer.MAX_VALUE);
	}

	@Test
	public void sampleTimeoutTime2(){
		StepVerifier.withVirtualTime(this::scenario_sampleTimeoutTime2)
		            .thenAwait(Duration.ofSeconds(10))
		            .expectNext(1, 2, 3, 10)
		            .verifyComplete();
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxSampleTimeout<Integer, Integer> test = new FluxSampleTimeout<>(parent, v -> Flux.just(2), Queues.empty());

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    public void scanMain() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSampleTimeout.SampleTimeoutMain<Integer, Integer> test =
        		new FluxSampleTimeout.SampleTimeoutMain<>(actual, i -> Flux.just(i),
        				Queues.<SampleTimeoutOther<Integer, Integer>>one().get());
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        test.requested = 35;
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35L);
        test.queue.add(new FluxSampleTimeout.SampleTimeoutOther<Integer, Integer>(test, 1, 0));
        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.error = new IllegalStateException("boom");
        assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
        test.onComplete();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
    public void scanOther() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSampleTimeout.SampleTimeoutMain<Integer, Integer> main =
        		new FluxSampleTimeout.SampleTimeoutMain<>(actual, i -> Flux.just(i),
        				Queues.<SampleTimeoutOther<Integer, Integer>>one().get());
        FluxSampleTimeout.SampleTimeoutOther<Integer, Integer> test =
        		new FluxSampleTimeout.SampleTimeoutOther<Integer, Integer>(main, 1, 0);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(main.other);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
        test.request(35);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onComplete();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }
}
