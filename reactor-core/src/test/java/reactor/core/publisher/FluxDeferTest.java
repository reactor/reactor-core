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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.context.Context;

public class FluxDeferTest {

	@Test(expected = NullPointerException.class)
	public void supplierNull() {
		Flux.<Integer>defer(null);
	}

	@Test
	public void supplierReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>defer(() -> null).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void supplierThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>defer(() -> {
			throw new RuntimeException("forced failure");
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.defer(() -> Flux.just(1)).subscribe(ts);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}


	@Test
	public void deferStream(){
		AtomicInteger i = new AtomicInteger();

		Flux<Integer> source =
				Flux.defer(() -> Flux.just(i.incrementAndGet()));

		Assert.assertEquals(source.blockLast().intValue(), 1);
		Assert.assertEquals(source.blockLast().intValue(), 2);
		Assert.assertEquals(source.blockLast().intValue(), 3);
	}

	@Test
	public void deferFluxWithContext() {
		Flux<Integer> source = Flux
				.deferWithContext(ctx -> {
					AtomicInteger i = ctx.get("i");
					return Mono.just(i.incrementAndGet());
				})
				.subscriberContext(Context.of(
						"i", new AtomicInteger()
				));

		Assert.assertEquals(source.blockFirst().intValue(), 1);
		Assert.assertEquals(source.blockFirst().intValue(), 2);
		Assert.assertEquals(source.blockFirst().intValue(), 3);
	}
}
