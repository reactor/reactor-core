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

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

public class FluxSampleFirstTest {

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		FluxIdentityProcessor<Integer> sp1 = Processors.more().multicastNoBackpressure();
		FluxIdentityProcessor<Integer> sp2 = Processors.more().multicastNoBackpressure();
		FluxIdentityProcessor<Integer> sp3 = Processors.more().multicastNoBackpressure();

		sp1.sampleFirst(v -> v == 1 ? sp2 : sp3)
		   .subscribe(ts);

		sp1.onNext(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		sp1.onNext(2);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		sp2.onNext(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		sp1.onNext(3);

		ts.assertValues(1, 3)
		  .assertNoError()
		  .assertNotComplete();

		sp1.onComplete();

		ts.assertValues(1, 3)
		  .assertNoError()
		  .assertComplete();

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp1 has subscribers?", sp2.hasDownstreams());
		Assert.assertFalse("sp1 has subscribers?", sp3.hasDownstreams());
	}

	@Test
	public void mainError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		FluxIdentityProcessor<Integer> sp1 = Processors.more().multicastNoBackpressure();
		FluxIdentityProcessor<Integer> sp2 = Processors.more().multicastNoBackpressure();
		FluxIdentityProcessor<Integer> sp3 = Processors.more().multicastNoBackpressure();

		sp1.sampleFirst(v -> v == 1 ? sp2 : sp3)
		   .subscribe(ts);

		sp1.onNext(1);
		sp1.onError(new RuntimeException("forced failure"));

		ts.assertValues(1)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp1 has subscribers?", sp2.hasDownstreams());
		Assert.assertFalse("sp1 has subscribers?", sp3.hasDownstreams());
	}

	@Test
	public void throttlerError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		FluxIdentityProcessor<Integer> sp1 = Processors.more().multicastNoBackpressure();
		FluxIdentityProcessor<Integer> sp2 = Processors.more().multicastNoBackpressure();
		FluxIdentityProcessor<Integer> sp3 = Processors.more().multicastNoBackpressure();

		sp1.sampleFirst(v -> v == 1 ? sp2 : sp3)
		   .subscribe(ts);

		sp1.onNext(1);
		sp2.onError(new RuntimeException("forced failure"));

		ts.assertValues(1)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp1 has subscribers?", sp2.hasDownstreams());
		Assert.assertFalse("sp1 has subscribers?", sp3.hasDownstreams());
	}

	@Test
	public void throttlerThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		FluxIdentityProcessor<Integer> sp1 = Processors.more().multicastNoBackpressure();

		sp1.sampleFirst(v -> {
			throw new RuntimeException("forced failure");
		})
		   .subscribe(ts);

		sp1.onNext(1);

		ts.assertValues(1)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
	}

	@Test
	public void throttlerReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		FluxIdentityProcessor<Integer> sp1 = Processors.more().multicastNoBackpressure();

		sp1.sampleFirst(v -> null)
		   .subscribe(ts);

		sp1.onNext(1);

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
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
	    FluxSampleFirst<Integer, Integer> test = new FluxSampleFirst(Flux.just(1), i -> i);

		Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    public void scanMainSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSampleFirst.SampleFirstMain<Integer, Integer> test =
        		new FluxSampleFirst.SampleFirstMain<>(actual, i -> Flux.just(i));
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
        test.requested = 35;
        Assertions.assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35L);

        test.error = new IllegalStateException("boom");
        Assertions.assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");

        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
    public void scanOtherSubscriber() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSampleFirst.SampleFirstMain<Integer, Integer> main =
        		new FluxSampleFirst.SampleFirstMain<>(actual, i -> Flux.just(i));
        FluxSampleFirst.SampleFirstOther<Integer> test = new  FluxSampleFirst.SampleFirstOther<>(main);

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(main.other);
        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
        Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
        test.request(35);
		Assertions.assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);

        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }
}
