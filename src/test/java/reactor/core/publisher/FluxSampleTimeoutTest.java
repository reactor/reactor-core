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

import org.junit.Assert;
import org.junit.Test;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

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
		           .delayMillis(300)
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
		           .delayMillis(300)
		           .sampleTimeout(d -> Mono.delay(Duration.ofMillis(100*d)), Integer.MAX_VALUE);
	}

	@Test
	public void sampleTimeoutTime2(){
		StepVerifier.withVirtualTime(this::scenario_sampleTimeoutTime2)
		            .thenAwait(Duration.ofSeconds(10))
		            .expectNext(1, 2, 3, 10)
		            .verifyComplete();
	}


}
