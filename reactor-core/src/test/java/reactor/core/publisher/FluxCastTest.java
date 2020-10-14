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
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class FluxCastTest {

	@Test
	public void sourceNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.just(1)
					.cast(null);
		});
	}

	@Test
	public void sourceNull2() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.just(1)
					.ofType(null);
		});
	}

	@Test
	public void normal() {
		StepVerifier.create(Flux.just(1)
		                        .cast(Number.class))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void error() {
		StepVerifier.create(Flux.just(1)
		                        .cast(String.class))
		            .verifyError(ClassCastException.class);
	}



	@Test
	public void normalOfType() {
		StepVerifier.create(Flux.just(1)
		                        .ofType(Number.class))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void errorOfType() {
		StepVerifier.create(Flux.just(1)
		                        .ofType(String.class))
		            .verifyComplete();
	}


}
