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
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class FluxSampleTest {

	@Test
	public void sourceNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new FluxSample<>(null, Flux.never());
		});
	}

	@Test
	public void otherNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.never().sample((Publisher<Object>) null);
		});
	}

	void sample(boolean complete, boolean which) {
		Sinks.Many<Integer> main = Sinks.unsafe().many().multicast().directBestEffort();

		Sinks.Many<String> other = Sinks.unsafe().many().multicast().directBestEffort();

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		main.asFlux().sample(other.asFlux()).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		main.emitNext(1, FAIL_FAST);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		other.emitNext("first", FAIL_FAST);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		other.emitNext("second", FAIL_FAST);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		main.emitNext(2, FAIL_FAST);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		other.emitNext("third", FAIL_FAST);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		Sinks.Many<?> p = which ? main : other;

		if (complete) {
			p.emitComplete(FAIL_FAST);

			ts.assertValues(1, 2)
			  .assertComplete()
			  .assertNoError();
		}
		else {
			p.emitError(new RuntimeException("forced failure"), FAIL_FAST);

			ts.assertValues(1, 2)
			  .assertNotComplete()
			  .assertError(RuntimeException.class)
			  .assertErrorMessage("forced failure");
		}

		assertThat(main.currentSubscriberCount()).as("main has subscriber").isZero();
		assertThat(other.currentSubscriberCount()).as("other has subscriber").isZero();
	}

	@Test
	public void normal1() {
		sample(true, false);
	}

	@Test
	public void normal2() {
		sample(true, true);
	}

	@Test
	public void error1() {
		sample(false, false);
	}

	@Test
	public void error2() {
		sample(false, true);
	}

	@Test
	public void subscriberCancels() {
		Sinks.Many<Integer> main = Sinks.unsafe().many().multicast().directBestEffort();

		Sinks.Many<String> other = Sinks.unsafe().many().multicast().directBestEffort();

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		main.asFlux().sample(other.asFlux()).subscribe(ts);

		assertThat(main.currentSubscriberCount()).as("main has subscriber").isPositive();
		assertThat(other.currentSubscriberCount()).as("other has subscriber").isPositive();

		ts.cancel();

		assertThat(main.currentSubscriberCount()).as("main has subscriber").isZero();
		assertThat(other.currentSubscriberCount()).as("other has subscriber").isZero();

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();
	}

	public void completeImmediately(boolean which) {
		Sinks.Many<Integer> main = Sinks.unsafe().many().multicast().directBestEffort();

		Sinks.Many<String> other = Sinks.unsafe().many().multicast().directBestEffort();

		if (which) {
			main.emitComplete(FAIL_FAST);
		}
		else {
			other.emitComplete(FAIL_FAST);
		}

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		main.asFlux().sample(other.asFlux()).subscribe(ts);

		assertThat(main.currentSubscriberCount()).as("main has subscriber").isZero();
		assertThat(other.currentSubscriberCount()).as("other has subscriber").isZero();

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void mainCompletesImmediately() {
		completeImmediately(true);
	}

	@Test
	public void otherCompletesImmediately() {
		completeImmediately(false);
	}

	@Test
	public void sampleIncludesLastItem() {
		Flux<Integer> source = Flux.concat(
				Flux.range(1, 5),
				Mono.delay(Duration.ofMillis(300)).ignoreElement().map(Long::intValue),
				Flux.just(80, 90, 100)
		).hide();

		Duration duration = StepVerifier.create(source.sample(Duration.ofMillis(250)))
		                                .expectNext(5)
		                                .expectNext(100)
		                                .verifyComplete();

		//sanity check on the sequence duration
		assertThat(duration.toMillis()).isLessThan(500);
	}

	@Test
	public void sourceTerminatesBeforeSamplingEmits() {
		Flux<Integer> source = Flux.just(1, 2).hide();

		Duration duration = StepVerifier.create(source.sample(Duration.ofMillis(250)))
		                                .expectNext(2)
		                                .verifyComplete();

		//sanity check on the sequence duration
		assertThat(duration.toMillis()).isLessThan(250);
	}

	@Test
	public void sourceErrorsBeforeSamplingNoEmission() {
		Flux<Integer> source = Flux.just(1, 2).concatWith(Mono.error(new IllegalStateException("boom")));

		Duration duration = StepVerifier.create(source.sample(Duration.ofMillis(250)))
		                                .verifyErrorMessage("boom");

		//sanity check on the sequence duration
		assertThat(duration.toMillis()).isLessThan(250);
	}

	@Test
    public void scanMainSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSample.SampleMainSubscriber<Integer> test = new FluxSample.SampleMainSubscriber<>(actual);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
        test.requested = 35;
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35L);
        test.value = 5;
        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
    public void scanOtherSubscriber() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSample.SampleMainSubscriber<Integer> main = new FluxSample.SampleMainSubscriber<>(actual);
        FluxSample.SampleOther<Integer, Integer> test = new FluxSample.SampleOther<>(main);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(main.other);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        main.cancelOther();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }
}
