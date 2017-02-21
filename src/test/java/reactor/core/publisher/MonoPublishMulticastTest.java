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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

public class MonoPublishMulticastTest {

	@Test
	public void normal() {
		AtomicInteger i = new AtomicInteger();
		Mono<Integer> m = Mono.fromCallable(i::incrementAndGet)
		                      .publish(o -> o.map(s -> 2));

		StepVerifier.create(m)
		            .expectNext(2)
		            .verifyComplete();

		StepVerifier.create(m)
		            .expectNext(2)
		            .verifyComplete();
	}

	@Test
	public void normalHide() {
		AtomicInteger i = new AtomicInteger();
		Mono<Integer> m = Mono.fromCallable(i::incrementAndGet)
		                      .hide()
		                      .publish(o -> o.map(s -> 2));

		StepVerifier.create(m)
		            .expectNext(2)
		            .verifyComplete();

		StepVerifier.create(m)
		            .expectNext(2)
		            .verifyComplete();
	}

	@Test
	public void cancelComposes() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		MonoProcessor<Integer> sp = MonoProcessor.create();

		sp.publish(o -> Mono.<Integer>never())
		  .subscribe(ts);

		Assert.assertTrue("Not subscribed?", sp.downstreamCount() != 0);

		ts.cancel();

		Assert.assertFalse("Still subscribed?", sp.isCancelled());
	}

	@Test
	public void cancelComposes2() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		MonoProcessor<Integer> sp = MonoProcessor.create();

		sp.publish(o -> Mono.<Integer>empty())
		  .subscribe(ts);

		Assert.assertFalse("Still subscribed?", sp.isCancelled());
	}


    @Test
    public void syncCancelBeforeComplete() {
        assertThat(Mono.just(Mono.just(1).publish(v -> v)).flatMap(v -> v).blockLast()).isEqualTo(1);
    }

    @Test
    public void normalCancelBeforeComplete() {
        assertThat(Mono.just(Mono.just(1).hide().publish(v -> v)).flatMap(v -> v).blockLast()).isEqualTo(1);
    }

}
