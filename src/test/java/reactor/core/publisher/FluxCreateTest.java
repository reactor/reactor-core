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

import org.junit.Test;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

public class FluxCreateTest {

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Flux<Integer> source = Flux.<Signal<Integer>>create(e -> {
			e.serialize().next(Signal.next(1));
			e.next(Signal.next(2));
			e.next(Signal.next(3));
			e.next(Signal.complete());
			System.out.println(e.isCancelled());
			System.out.println(e.requestedFromDownstream());
		}).dematerialize();

		source.subscribe(ts);

		ts.assertValues(1, 2, 3)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void createStreamFromFluxCreate() {
		StepVerifier.create(Flux.create(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}))
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}

	@Test
	public void createStreamFromFluxCreate2(){
		StepVerifier.create(Flux.create(s -> {
			s.next("test1");
			s.next("test2");
			s.next("test3");
			s.complete();
		}).publishOn(Schedulers.parallel()))
		            .expectNext("test1", "test2", "test3")
		            .verifyComplete();
	}
}