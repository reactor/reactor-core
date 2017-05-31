/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
import reactor.core.Scannable;
import reactor.test.StepVerifier;

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
	public void fluxEmpty() {
		StepVerifier.create(Mono.empty().flux())
		            .verifyComplete();
	}

	@Test
	public void scanMain() {
		Flux<Integer> parent = Flux.range(1,  10);
		FluxSource<Integer, Integer> test = new FluxSource<>(parent);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(-1);
	}

}