/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MonoToOptionalTest {

	@Test
	public void empty() {
		Mono<String> source = Mono.empty();
		Optional<String> optional = source.toOptional();

		assertThat(optional).isEmpty();
	}

	@Test
	public void valued() {
		Mono<String> source = Mono.just("foo");
		Optional<String> optional = source.toOptional();

		assertThat(optional)
				.isNotEmpty()
	            .contains("foo");
	}

	@Test
	public void error() {
		Mono<String> source = Mono.error(new IllegalArgumentException("boom"));

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(source::toOptional)
		        .withMessage("boom");
	}

	@Test
	public void delayedBlocks() {
		Mono<String> source = Mono.just("foo")
		                          .delayElement(Duration.ofMillis(500));

		long start = System.nanoTime();
		Optional<String> optional = source.toOptional();

		assertThat(optional)
				.isNotEmpty()
				.contains("foo");

		assertThat(System.nanoTime() - start)
				.overridingErrorMessage("toOptional took more than 500ms")
				.isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(500));
	}

	@Test
	public void delayedTimesOut() {
		Mono<String> source = Mono.just("foo")
		                          .delayElement(Duration.ofMillis(500));

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(() -> source.toOptional(Duration.ofMillis(400)))
	            .withMessage("Timeout on blocking read for 400 MILLISECONDS");
	}
}