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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MonoToCompletableFutureTest {

	@Test
	public void normal() throws Exception {
		CompletableFuture<Integer> f = Mono.just(1)
		                                   .toFuture();

		assertThat(f.get()).isEqualTo(1);
	}

	@Test
	public void error() {
		CompletableFuture<Integer> f =
				Mono.<Integer>error(new IllegalStateException("test")).toFuture();

		assertThat(f.isDone()).isTrue();
		assertThat(f.isCompletedExceptionally()).isTrue();

		assertThatExceptionOfType(ExecutionException.class)
				.isThrownBy(f::get)
				.withCauseExactlyInstanceOf(IllegalStateException.class)
				.withMessage("java.lang.IllegalStateException: test");
	}

	@Test
	public void empty() throws Exception {
		CompletableFuture<Integer> f = Mono.<Integer>empty().toFuture();

		assertThat(f.get()).isNull();
	}

	@Test
	public void monoSourceIsntCancelled() {
		AtomicBoolean flag = new AtomicBoolean();

		assertThat(Mono.just("value")
		    .doOnCancel(() -> flag.set(true))
		    .toFuture()
		).isCompletedWithValue("value");
		
		assertThat(flag).as("cancelled").isFalse();
	}

	@Test
	public void sourceCanBeCancelledExplicitlyByOnNext() {
		AtomicBoolean flag = new AtomicBoolean();

		assertThat(Flux.just("value")
		               .doOnCancel(() -> flag.set(true))
		               .subscribeWith(new MonoToCompletableFuture<>(true))
		).isCompletedWithValue("value");

		assertThat(flag).as("cancelled").isTrue();
	}
}
