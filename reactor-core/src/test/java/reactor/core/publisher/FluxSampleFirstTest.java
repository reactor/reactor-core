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
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxSampleFirstTest {

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp3 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux()
		   .sampleFirst(v -> v == 1 ? sp2.asFlux() : sp3.asFlux())
		   .subscribe(ts);

		sp1.emitNext(1, FAIL_FAST);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		sp1.emitNext(2, FAIL_FAST);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		sp2.emitNext(1, FAIL_FAST);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		sp1.emitNext(3, FAIL_FAST);

		ts.assertValues(1, 3)
		  .assertNoError()
		  .assertNotComplete();

		sp1.emitComplete(FAIL_FAST);

		ts.assertValues(1, 3)
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
		Sinks.Many<Integer> sp3 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux()
		   .sampleFirst(v -> v == 1 ? sp2.asFlux() : sp3.asFlux())
		   .subscribe(ts);

		sp1.emitNext(1, FAIL_FAST);
		sp1.emitError(new RuntimeException("forced failure"), FAIL_FAST);

		ts.assertValues(1)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		assertThat(sp1.currentSubscriberCount()).as("sp1 has subscriber").isZero();
		assertThat(sp2.currentSubscriberCount()).as("sp2 has subscriber").isZero();
		assertThat(sp3.currentSubscriberCount()).as("sp3 has subscriber").isZero();
	}

	@Test
	public void throttlerError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp3 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux()
		   .sampleFirst(v -> v == 1 ? sp2.asFlux() : sp3.asFlux())
		   .subscribe(ts);

		sp1.emitNext(1, FAIL_FAST);
		sp2.emitError(new RuntimeException("forced failure"), FAIL_FAST);

		ts.assertValues(1)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		assertThat(sp1.currentSubscriberCount()).as("sp1 has subscriber").isZero();
		assertThat(sp2.currentSubscriberCount()).as("sp2 has subscriber").isZero();
		assertThat(sp3.currentSubscriberCount()).as("sp3 has subscriber").isZero();
	}

	@Test
	public void throttlerThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux()
		   .sampleFirst(v -> {
			throw new RuntimeException("forced failure");
		})
		   .subscribe(ts);

		sp1.emitNext(1, FAIL_FAST);

		ts.assertValues(1)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		assertThat(sp1.currentSubscriberCount()).as("sp1 has subscriber").isZero();
	}

	@Test
	public void throttlerReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux()
		   .sampleFirst(v -> null)
		   .subscribe(ts);

		sp1.emitNext(1, FAIL_FAST);

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();

		assertThat(sp1.currentSubscriberCount()).as("sp1 has subscriber").isZero();
	}

	Flux<Integer> scenario_sampleFirstTime(){
		return Flux.range(1, 10)
	        .delayElements(Duration.ofMillis(200))
	        .sampleFirst(Duration.ofSeconds(1));
	}

	@Test
	public void sampleFirstTime(){
		StepVerifier.withVirtualTime(this::scenario_sampleFirstTime)
		            .thenAwait(Duration.ofSeconds(10))
	                .expectNext(1, 6)
	                .verifyComplete();
	}

	@Test
	public void scanOperator(){
	    FluxSampleFirst<Integer, Integer> test = new FluxSampleFirst<>(Flux.just(1), i -> Flux.just(i));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    public void scanMainSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSampleFirst.SampleFirstMain<Integer, Integer> test =
        		new FluxSampleFirst.SampleFirstMain<>(actual, i -> Flux.just(i));
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
        test.requested = 35;
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35L);

        test.error = new IllegalStateException("boom");
        assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
    public void scanOtherSubscriber() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSampleFirst.SampleFirstMain<Integer, Integer> main =
        		new FluxSampleFirst.SampleFirstMain<>(actual, i -> Flux.just(i));
        FluxSampleFirst.SampleFirstOther<Integer> test = new  FluxSampleFirst.SampleFirstOther<>(main);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(main.other);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
        test.request(35);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }
}
