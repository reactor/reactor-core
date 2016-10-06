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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;
import reactor.test.subscriber.AssertSubscriber;

public class FluxOnBackpressureDropTest {

	@Test(expected = NullPointerException.class)
	public void source1Null() {
		new FluxOnBackpressureDrop<>(null);
	}

	@Test(expected = NullPointerException.class)
	public void source2Null() {
		new FluxOnBackpressureDrop<>(null, v -> {
		});
	}

	@Test(expected = NullPointerException.class)
	public void onDropNull() {
		Flux.never().onBackpressureDrop(null);
	}

	@Test
	public void onDropAssigned() {
		Consumer<Object> onDrop = c -> {};
		PublisherMock<?> p = PublisherMock.create();
		p.flux().onBackpressureDrop(onDrop).subscribe();
		Assert.assertTrue(p.get(0).connectedInput().equals(onDrop));
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .onBackpressureDrop()
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .onBackpressureError()
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .onBackpressureDrop()
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void someDrops() {
		DirectProcessor<Integer> tp = DirectProcessor.create();

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		List<Integer> drops = new ArrayList<>();

		tp.onBackpressureDrop(drops::add)
		  .subscribe(ts);

		tp.onNext(1);

		ts.request(2);

		tp.onNext(2);
		tp.onNext(3);
		tp.onNext(4);

		ts.request(1);

		tp.onNext(5);
		tp.onComplete();

		ts.assertValues(2, 3, 5)
		  .assertComplete()
		  .assertNoError();

		Assert.assertEquals(Arrays.asList(1, 4), drops);
	}

	@Test
	public void onDropThrows() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .onBackpressureDrop(e -> {
			    throw new RuntimeException("forced failure");
		    })
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}
}
