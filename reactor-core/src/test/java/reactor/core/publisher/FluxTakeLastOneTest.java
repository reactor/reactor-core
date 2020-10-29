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

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class FluxTakeLastOneTest {

	@Test
	public void empty() {
		Flux<?> f = Flux.empty()
		                .takeLast(1);

		assertThat(f.getPrefetch()).isEqualTo(Integer.MAX_VALUE);

		StepVerifier.create(f)
	                .verifyComplete();
	}

	@Test
	public void error() {
		StepVerifier.create(Flux.error(new Exception("test"))
		                        .takeLast(1))
	                .verifyErrorMessage("test");
	}

	@Test
	public void illegal() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
			StepVerifier.create(Flux.empty()
					.takeLast(-1))
					.verifyComplete();
		});
	}

	@Test
	public void normal() {
		StepVerifier.create(Flux.range(1, 100)
		                        .takeLast(1))
	                .expectNext(100)
	                .verifyComplete();
	}

	@Test
	public void normalHide() {
		StepVerifier.create(Flux.range(1, 100)
		                        .hide()
		                        .takeLast(1))
	                .expectNext(100)
	                .verifyComplete();
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1, 2, 3);
		FluxTakeLastOne<Integer> test = new FluxTakeLastOne<>(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
