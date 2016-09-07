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

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.test.TestSubscriber;

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
		DirectProcessor<Integer> main = DirectProcessor.create();

		DirectProcessor<String> other = DirectProcessor.create();

		TestSubscriber<Integer> ts = TestSubscriber.create();

		main.sample(other).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		main.onNext(1);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		other.onNext("first");

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		other.onNext("second");

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		main.onNext(2);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		other.onNext("third");

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		DirectProcessor<?> p = which ? main : other;

		if (complete) {
			p.onComplete();

			ts.assertValues(1, 2)
			  .assertComplete()
			  .assertNoError();
		}
		else {
			p.onError(new RuntimeException("forced failure"));

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
		DirectProcessor<Integer> main = DirectProcessor.create();

		DirectProcessor<String> other = DirectProcessor.create();

		TestSubscriber<Integer> ts = TestSubscriber.create();

		main.sample(other).subscribe(ts);

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
		DirectProcessor<Integer> main = DirectProcessor.create();

		DirectProcessor<String> other = DirectProcessor.create();

		if (which) {
			main.onComplete();
		}
		else {
			other.onComplete();
		}

		TestSubscriber<Integer> ts = TestSubscriber.create();

		main.sample(other).subscribe(ts);

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

}
