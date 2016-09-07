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

import java.util.Collections;

import org.junit.Test;
import reactor.core.Fuseable;
import reactor.test.TestSubscriber;

public class FluxCombineLatestTest {

	@Test
	public void singleSourceIsMapped() {
		
		TestSubscriber<String> ts = TestSubscriber.create();
		
		Flux.combineLatest(a -> a[0].toString(), Flux.just(1))
		.subscribe(ts);
		
		ts.assertValues("1")
		.assertNoError()
		.assertComplete();
	}
	
	@Test
	public void iterableSingleSourceIsMapped() {
		
		TestSubscriber<String> ts = TestSubscriber.create();
		
		Flux.combineLatest(Collections.singleton(Flux.just(1)), a -> a[0].toString())
		.subscribe(ts);
		
		ts.assertValues("1")
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void fused() {
		DirectProcessor<Integer> dp1 = DirectProcessor.create();
		DirectProcessor<Integer> dp2 = DirectProcessor.create();

		TestSubscriber<Integer> ts = TestSubscriber.create();
		ts.requestedFusionMode(Fuseable.ANY);

		Flux.combineLatest(dp1, dp2, (a, b) -> a + b)
		  .subscribe(ts);

		dp1.onNext(1);
		dp1.onNext(2);

		dp2.onNext(10);
		dp2.onNext(20);
		dp2.onNext(30);

		dp1.onNext(3);

		dp1.onComplete();
		dp2.onComplete();

		ts.assertFuseableSource()
		  .assertFusionMode(Fuseable.ASYNC)
		  .assertValues(12, 22, 32, 33);
	}
}
