/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.core.publisher.FluxSampleTimeout.SampleTimeoutOther;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.QueueSupplier;

public class FluxSampleTimeoutTest {

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();
		DirectProcessor<Integer> sp3 = DirectProcessor.create();

		sp1.sampleTimeout(v -> v == 1 ? sp2 : sp3)
		   .subscribe(ts);

		sp1.onNext(1);
		sp2.onNext(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		sp1.onNext(2);
		sp1.onNext(3);
		sp1.onNext(4);

		sp3.onNext(2);

		ts.assertValues(1, 4)
		  .assertNoError()
		  .assertNotComplete();

		sp1.onNext(5);
		sp1.onComplete();

		ts.assertValues(1, 4, 5)
		  .assertNoError()
		  .assertComplete();

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());
		Assert.assertFalse("sp3 has subscribers?", sp3.hasDownstreams());
	}

	@Test
	public void mainError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		sp1.sampleTimeout(v -> sp2)
		   .subscribe(ts);

		sp1.onNext(1);
		sp1.onError(new RuntimeException("forced failure"));

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());
	}

	@Test
	public void throttlerError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		sp1.sampleTimeout(v -> sp2)
		   .subscribe(ts);

		sp1.onNext(1);
		sp2.onError(new RuntimeException("forced failure"));

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());
	}

	@Test
	public void throttlerReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();

		sp1.sampleTimeout(v -> null)
		   .subscribe(ts);

		sp1.onNext(1);

		ts.assertNoValues()
		  .assertError(NullPointerException.class)
		  .assertNotComplete();

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
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
    public void scanMain() {
        Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSampleTimeout.SampleTimeoutMain<Integer, Integer> test =
        		new FluxSampleTimeout.SampleTimeoutMain<>(actual, i -> Flux.just(i),
        				QueueSupplier.<SampleTimeoutOther<Integer, Integer>>one().get());
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
        test.requested = 35;
        Assertions.assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35L);
        test.queue.add(new FluxSampleTimeout.SampleTimeoutOther<Integer, Integer>(test, 1, 0));
        Assertions.assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(1);

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        test.error = new IllegalStateException("boom");
        Assertions.assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).hasMessage("boom");
        test.onComplete();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }

	@Test
    public void scanOther() {
		Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSampleTimeout.SampleTimeoutMain<Integer, Integer> main =
        		new FluxSampleTimeout.SampleTimeoutMain<>(actual, i -> Flux.just(i),
        				QueueSupplier.<SampleTimeoutOther<Integer, Integer>>one().get());
        FluxSampleTimeout.SampleTimeoutOther<Integer, Integer> test =
        		new FluxSampleTimeout.SampleTimeoutOther<Integer, Integer>(main, 1, 0);

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(main.other);
        Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(main);
        test.request(35);;
        Assertions.assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        test.onComplete();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        test.cancel();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }
}
