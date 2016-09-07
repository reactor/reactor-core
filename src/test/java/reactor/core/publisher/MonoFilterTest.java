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

import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.Test;
import org.testng.Assert;
import reactor.core.Exceptions;
import reactor.test.TestSubscriber;

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
		    .subscribeWith(TestSubscriber.create())
		    .assertNoValues()
		    .assertComplete()
		    .assertNoError();

		Mono.just(1)
		    .filter(v -> v % 2 != 0)
		    .subscribeWith(TestSubscriber.create())
		    .assertValues(1)
		    .assertComplete()
		    .assertNoError();
	}

	@Test
	public void normalBackpressuredJust() {
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

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
		TestSubscriber<Integer> ts = TestSubscriber.create(0);

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
		TestSubscriber<Object> ts = TestSubscriber.create(2);

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
		TestSubscriber<Object> ts = TestSubscriber.create();

		Mono.just(2)
		    .filter(v -> (v & 1) == 0)
		    .subscribe(ts);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void asyncFusion() {
		TestSubscriber<Object> ts = TestSubscriber.create();

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
		TestSubscriber<Object> ts = TestSubscriber.create(1);

		MonoProcessor<Integer> up = MonoProcessor.create();

		Mono.just(1)
		    .hide()
		    .then(w -> up.filter(v -> (v & 1) == 0))
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

}
