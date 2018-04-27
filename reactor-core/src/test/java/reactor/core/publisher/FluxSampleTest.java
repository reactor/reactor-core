/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxSampleTest {

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxSample<>(null, Flux.never());
	}

	@Test(expected = NullPointerException.class)
	public void otherNull() {
		Flux.never().sample((Publisher<Object>)null);
	}

	void sample(boolean complete, boolean which) {
		FluxProcessorSink<Integer> main = Processors.direct();

		FluxProcessorSink<String> other = Processors.direct();

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		main.asFlux().sample(other.asFlux()).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		main.next(1);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		other.next("first");

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		other.next("second");

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		main.next(2);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		other.next("third");

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		FluxProcessorSink<?> p = which ? main : other;

		if (complete) {
			p.complete();

			ts.assertValues(1, 2)
			  .assertComplete()
			  .assertNoError();
		}
		else {
			p.error(new RuntimeException("forced failure"));

			ts.assertValues(1, 2)
			  .assertNotComplete()
			  .assertError(RuntimeException.class)
			  .assertErrorMessage("forced failure");
		}

		Assert.assertFalse("Main has subscribers?", main.hasDownstreams());
		Assert.assertFalse("Other has subscribers?", other.hasDownstreams());
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
		FluxProcessorSink<Integer> main = Processors.direct();

		FluxProcessorSink<String> other = Processors.direct();

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		main.asFlux().sample(other.asFlux()).subscribe(ts);

		Assert.assertTrue("Main no subscriber?", main.hasDownstreams());
		Assert.assertTrue("Other no subscriber?", other.hasDownstreams());

		ts.cancel();

		Assert.assertFalse("Main no subscriber?", main.hasDownstreams());
		Assert.assertFalse("Other no subscriber?", other.hasDownstreams());

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();
	}

	public void completeImmediately(boolean which) {
		FluxProcessorSink<Integer> main = Processors.direct();

		FluxProcessorSink<String> other = Processors.direct();

		if (which) {
			main.complete();
		}
		else {
			other.complete();
		}

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		main.asFlux().sample(other.asFlux()).subscribe(ts);

		Assert.assertFalse("Main subscriber?", main.hasDownstreams());
		Assert.assertFalse("Other subscriber?", other.hasDownstreams());

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
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        main.cancelOther();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }
}
