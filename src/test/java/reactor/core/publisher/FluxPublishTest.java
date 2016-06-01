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
import reactor.core.flow.Fuseable;
import reactor.core.queue.QueueSupplier;
import reactor.core.test.TestSubscriber;
import reactor.core.tuple.Tuple;
import reactor.core.tuple.Tuple2;

import static reactor.core.publisher.Flux.range;
import static reactor.core.publisher.Flux.zip;

public class FluxPublishTest {

	@Test
	public void subsequentSum() {

		TestSubscriber<Integer> ts = TestSubscriber.create();

		range(1, 5).publish(o -> zip((Object[] a) -> (Integer) a[0] + (Integer) a[1], o, o.skip(1)))
		           .subscribe(ts);

		ts.assertValues(1 + 2, 2 + 3, 3 + 4, 4 + 5)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void subsequentSumHidden() {

		TestSubscriber<Integer> ts = TestSubscriber.create();

		range(1, 5).hide()
		           .publish(o -> zip((Object[] a) -> (Integer) a[0] + (Integer) a[1], o, o
				           .skip(1)))
		           .subscribe(ts);

		ts.assertValues(1 + 2, 2 + 3, 3 + 4, 4 + 5)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void subsequentSumAsync() {

		TestSubscriber<Integer> ts = TestSubscriber.create();

		UnicastProcessor<Integer> up =
				new UnicastProcessor<>(QueueSupplier.<Integer>get(16).get());

		up.publish(o -> zip((Object[] a) -> (Integer) a[0] + (Integer) a[1], o, o.skip(1)))
		  .subscribe(ts);

		up.onNext(1);
		up.onNext(2);
		up.onNext(3);
		up.onNext(4);
		up.onNext(5);
		up.onComplete();

		ts.assertValues(1 + 2, 2 + 3, 3 + 4, 4 + 5)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void cancelComposes() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		EmitterProcessor<Integer> sp = EmitterProcessor.create();

		sp.publish(o -> Flux.<Integer>never())
		  .subscribe(ts);

		Assert.assertTrue("Not subscribed?", sp.downstreamCount() != 0);

		ts.cancel();

		Assert.assertFalse("Still subscribed?", sp.downstreamCount() == 0);
	}

	@Test
	public void cancelComposes2() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		EmitterProcessor<Integer> sp = EmitterProcessor.create();

		sp.publish(o -> Flux.<Integer>empty())
		  .subscribe(ts);

		Assert.assertFalse("Still subscribed?", sp.downstreamCount() == 0);
	}

	@Test
	public void pairWise() {
		TestSubscriber<Tuple2<Integer, Integer>> ts = TestSubscriber.create();

		range(1, 9).compose(o -> zip(o, o.skip(1)))
		           .subscribe(ts);

		ts.assertValues(Tuple.of(1, 2),
				Tuple.of(2, 3),
				Tuple.of(3, 4),
				Tuple.of(4, 5),
				Tuple.of(5, 6),
				Tuple.of(6, 7),
				Tuple.of(7, 8),
				Tuple.of(8, 9))
		  .assertComplete();
	}

	@Test
	public void innerCanFuse() {
		TestSubscriber<Integer> ts = TestSubscriber.create();
		ts.requestedFusionMode(Fuseable.ANY);

		Flux.never()
		    .publish(o -> range(1, 5))
		    .subscribe(ts);

		ts.assertFuseableSource()
		  .assertFusionMode(Fuseable.SYNC)
		  .assertValues(1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();
	}
}
