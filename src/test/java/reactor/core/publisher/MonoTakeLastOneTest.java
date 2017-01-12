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
import reactor.test.StepVerifier;

public class MonoTakeLastOneTest {

	@Test
	public void empty() {
		StepVerifier.create(Flux.empty()
		                        .last())
		            .verifyComplete();
	}

	@Test
	public void fallback() {
		StepVerifier.create(Flux.empty()
		                        .last(1))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void error() {
		StepVerifier.create(Flux.error(new Exception("test"))
		                        .last())
		            .verifyErrorMessage("test");
	}

	@Test
	public void normal() {
		StepVerifier.create(Flux.range(1, 100)
		                        .last())
		            .expectNext(100)
		            .verifyComplete();
	}

	@Test
	public void normal2() {
		StepVerifier.create(Flux.range(1, 100)
		                        .last(-1))
		            .expectNext(100)
		            .verifyComplete();
	}


	@Test
	public void normal3() {
		StepVerifier.create(Mono.fromCallable(() -> 100)
		                        .flux()
		                        .last(-1))
		            .expectNext(100)
		            .verifyComplete();
	}

	@Test
	public void normalHide() {
		StepVerifier.create(Flux.range(1, 100)
		                        .hide()
		                        .last())
		            .expectNext(100)
		            .verifyComplete();
	}

	@Test
	public void norma2() {
		StepVerifier.create(Flux.just(100)
		                        .last(-1))
		            .expectNext(100)
		            .verifyComplete();
	}
}