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
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class FluxSourceTest {

	@Test
	public void wrapToFlux(){
		MonoProcessor<String> mp = MonoProcessor.create();

		mp.onNext("test");
		StepVerifier.create(Flux.from(mp))
		            .expectNext("test")
		            .verifyComplete();
	}

	@Test
	public void empty() {
		Flux<Integer> m = Flux.from(Mono.empty());
		assertTrue(m == Flux.<Integer>empty());
		StepVerifier.create(m)
		            .verifyComplete();
	}

	@Test
	public void just() {
		Flux<Integer> m = Flux.from(Mono.just(1));
		assertTrue(m instanceof FluxJust);
		StepVerifier.create(m)
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void error() {
		Flux<Integer> m = Flux.from(Mono.error(new Exception("test")));
		assertTrue(m instanceof FluxError);
		StepVerifier.create(m)
		            .verifyErrorMessage("test");
	}

	@Test
	public void errorPropagate() {
		Flux<Integer> m = Flux.from(Mono.error(new Error("test")));
		assertTrue(m instanceof FluxError);
		StepVerifier.create(m)
		            .verifyErrorMessage("test");
	}


	@Test
	public void wrap() {
		Flux<Integer> m = Flux.wrap(Flux.just(1));
		StepVerifier.create(m)
		            .expectNext(1)
		            .verifyComplete();

		m = Flux.wrap(Flux.just(1).hide());
		StepVerifier.create(m)
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void asJust() {
		StepVerifier.create(Mono.just(1).as(Flux::from))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void fluxJust() {
		StepVerifier.create(Mono.just(1).flux())
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void fluxError() {
		StepVerifier.create(Mono.error(new Exception("test")).flux())
		            .verifyErrorMessage("test");
	}


	@Test
	public void fluxEmpty() {
		StepVerifier.create(Mono.empty().flux())
		            .verifyComplete();
	}

	@Test
	public void scanOperatorWithSyncSource() {
		Flux<Integer> parent = Flux.range(1,  10).map(i -> i);
		FluxSource<Integer> test = new FluxSource(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(-1);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanOperatorWithAsyncSource(){
		FluxDelaySequence<String> source = new FluxDelaySequence(Flux.just(1), Duration.ofMillis(50), Schedulers.immediate());
		FluxSource<String> test = new FluxSource(source);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(-1);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
	}

	@Test
	public void scanOperatorHide() {
		Flux<Integer> parent = Flux.range(1,  10).hide();
		FluxSource<Integer> test = new FluxSource<>(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(-1);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.UNKNOWN);
	}

	@Test
	public void scanFuseableOperatorWithSyncSource(){
		Flux<Integer> source = Flux.just(1);
		FluxSourceFuseable<Integer> test = new FluxSourceFuseable<>(source);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(-1);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanFuseableOperatorWithAsyncSource(){
		FluxDelaySequence<String> source = new FluxDelaySequence(Flux.just(1), Duration.ofMillis(50), Schedulers.immediate());
		FluxSourceFuseable<String> test = new FluxSourceFuseable(source);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(-1);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
	}
}