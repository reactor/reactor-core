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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxMergeTest {

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.merge(Flux.just(1),
				Flux.range(2, 2),
				Flux.just(4, 5, 6)
				    .hide())
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normal2() {
		StepVerifier.create(Mono.just(1).mergeWith(Flux.just(2, 3)))
		            .expectNext(1, 2, 3)
		            .verifyComplete();
	}

	@Test
	public void mergeWithNoStackoverflow() {
		int n = 5000;

		Flux<Integer> source = Flux.just(1);

		Flux<Integer> result = source;
		for (int i = 0; i < n; i++) {
			result = result.mergeWith(source);
		}

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		result.subscribe(ts);

		ts.assertValueCount(n + 1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void mergeEmpty(){
		StepVerifier.create(Flux.merge())
	                .verifyComplete();
	}


	@Test
	public void mergeOne(){
		StepVerifier.create(Flux.merge(Flux.just(1)))
		            .expectNext(1)
	                .verifyComplete();
	}

	@Test
	public void mergePublisherPublisher(){
		AtomicLong request = new AtomicLong();
		StepVerifier.create(Flux.merge(Flux.just(Flux.just(1, 2), Flux.just(3, 4)).doOnRequest(request::set)))
	                .expectNext(1, 2, 3, 4)
	                .then(() -> assertThat(request).hasValue(1L) )
	                .verifyComplete();
	}


	@Test
	public void mergePublisherPublisher2(){
		StepVerifier.create(Flux.merge(Flux.just(Flux.just(1, 2), Flux.just(3, 4)), 1))
	                .expectNext(1, 2, 3, 4)
	                .verifyComplete();
	}

	@Test
	public void mergePublisherPublisherIterable(){
		StepVerifier.create(Flux.merge(Arrays.asList(Flux.just(1, 2), Flux.just(3, 4))))
	                .expectNext(1, 2, 3, 4)
	                .verifyComplete();
	}

	@Test
	public void mergeDelayError() {
		IllegalStateException boom = new IllegalStateException("boom");

		StepVerifier.create(Flux.mergeDelayError(32,
				Flux.error(boom).hide(),
				Flux.range(1, 4)
		))
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorSatisfies(e -> assertThat(e).isEqualTo(boom));
	}

	//see https://github.com/reactor/reactor-core/issues/936
	@Test
	public void delayErrorWithFluxError() {
		StepVerifier.create(
				Flux.mergeDelayError(32, Flux.just(1, 2),
						Flux.error(new Exception("test")),
						Flux.just(3, 4))
		)
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorMessage("test");
	}

	//see https://github.com/reactor/reactor-core/issues/936
	@Test
	public void delayErrorWithMonoError() {
		StepVerifier.create(
				Flux.mergeDelayError(32,
						Flux.just(1, 2),
						Mono.error(new Exception("test")),
						Flux.just(3, 4))
				)
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorMessage("test");
	}

	@Test
	public void scanOperator() {
		@SuppressWarnings("unchecked")
		Publisher<String>[] sources = new Publisher[0];
		FluxMerge<String> s = new FluxMerge<>(sources, true, 3, Queues.small(), 123, Queues.small());
		assertThat(s.scan(Scannable.Attr.DELAY_ERROR)).as("delayError").isTrue();
		assertThat(s.scan(Scannable.Attr.PREFETCH)).as("prefetch").isEqualTo(123);
		assertThat(s.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
