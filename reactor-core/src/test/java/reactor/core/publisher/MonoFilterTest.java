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

import org.junit.Test;
import org.testng.Assert;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoFilterTest {

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new MonoFilter<Integer>(null, e -> true);
	}

	@Test(expected = NullPointerException.class)
	public void predicateNull() {
		Mono.never()
		    .filter(null);
	}

	@Test
	public void normal() {
		Mono.just(1)
		    .filter(v -> v % 2 == 0)
		    .subscribeWith(AssertSubscriber.create())
		    .assertNoValues()
		    .assertComplete()
		    .assertNoError();

		Mono.just(1)
		    .filter(v -> v % 2 != 0)
		    .subscribeWith(AssertSubscriber.create())
		    .assertValues(1)
		    .assertComplete()
		    .assertNoError();
	}

	@Test
	public void normalBackpressuredJust() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.just(1)
		    .filter(v -> v % 2 != 0)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressuredCallable() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.fromCallable(() -> 2)
		    .filter(v -> v % 2 == 0)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);

		ts.assertValues(2)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void predicateThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create(2);

		Mono.create(s -> s.success(1))
		    .filter(v -> {
			    throw new RuntimeException("forced failure");
		    })
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void syncFusion() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Mono.just(2)
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void asyncFusion() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		MonoProcessor<Integer> up = MonoProcessor.create();

		up.filter(v -> (v & 1) == 0)
		  .subscribe(ts);
		up.onNext(2);
		up.onComplete();

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void asyncFusionBackpressured() {
		AssertSubscriber<Object> ts = AssertSubscriber.create(1);

		MonoProcessor<Integer> up = MonoProcessor.create();

		Mono.just(1)
		    .hide()
		    .flatMap(w -> up.filter(v -> (v & 1) == 0))
		    .subscribe(ts);

		up.onNext(2);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();

		try{
			up.onNext(3);
		}
		catch(Exception e){
			Assert.assertTrue(Exceptions.isCancel(e));
		}

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void filterMono() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		StepVerifier.create(Mono.just(2).filter(s -> s % 2 == 0).subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isFalse())
		            .then(() -> assertThat(mp.isSuccess()).isTrue())
		            .then(() -> assertThat(mp.peek()).isEqualTo(2))
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .expectNext(2)
		            .verifyComplete();
	}


	@Test
	public void filterMonoNot() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		StepVerifier.create(Mono.just(1).filter(s -> s % 2 == 0).subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isFalse())
		            .then(() -> assertThat(mp.isSuccess()).isTrue())
		            .then(() -> assertThat(mp.peek()).isNull())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .verifyComplete();
	}

	@Test
	public void scanOperator() {
		MonoFilter<Integer> test = new MonoFilter<>(Mono.just(1), (v -> v % 2 != 0));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanFuseableOperator() {
		MonoFilterFuseable<Integer> test = new MonoFilterFuseable<>(Mono.just(1), (v -> v % 2 != 0));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
