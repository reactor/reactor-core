/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
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

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class MonoRepeatPredicateTest {

	//these tests essentially cover the API and its escape hatches
	@Test
	public void predicateNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Mono.never()
					.repeat(null);
		});
	}

	@Test
	public void nMinusOne() {
		Mono<Integer> source = Mono.just(3);

		assertThatIllegalArgumentException()
		          .isThrownBy(() -> source.repeat(-1, () -> true))
		          .withMessage("numRepeat >= 0 required");
	}

	@Test
	public void nZero() {
		StepVerifier.create(Mono.just(3)
		                        .repeat(0, () -> true))
		            .expectNext(3)
		            .verifyComplete();
	}

	@Test
	public void nOne() {
		StepVerifier.create(Mono.just(3)
		                        .repeat(1, () -> true))
		            .expectNext(3)
		            .expectNext(3)
		            .verifyComplete();
	}

	@Test
	public void nTwo() {
		StepVerifier.create(Mono.just(3)
		                        .repeat(2, () -> true))
		            .expectNext(3)
		            .expectNext(3)
		            .expectNext(3)
		            .verifyComplete();
	}

	@Test
	public void scanOperator(){
	    MonoRepeatPredicate<Integer> test = new MonoRepeatPredicate<>(Mono.just(1), () -> true);

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

}
