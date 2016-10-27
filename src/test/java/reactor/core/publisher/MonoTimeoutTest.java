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

import java.util.concurrent.TimeoutException;

import org.junit.Test;
import reactor.test.subscriber.AssertSubscriber;

public class MonoTimeoutTest {

	@Test
	public void noTimeout() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1)
		    .timeout(Mono.never())
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void immediateTimeout() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1)
		    .timeout(Mono.empty())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(TimeoutException.class);
	}

	@Test
	public void firstTimeoutError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1)
		    .timeout(Mono.error(new RuntimeException("forced " + "failure")))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void timeoutRequested() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		MonoProcessor<Integer> source = MonoProcessor.create();

		DirectProcessor<Integer> tp = DirectProcessor.create();

		source.timeout(tp)
		      .subscribe(ts);

		tp.onNext(1);

		source.onNext(2);
		source.onComplete();

		ts.assertNoValues()
		  .assertError(TimeoutException.class)
		  .assertNotComplete();
	}
}