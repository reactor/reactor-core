/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import reactor.core.Exceptions;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;

public class SerializedManySinkTest {

	@Test
	public void shouldNotThrowFromTryEmitNext() {
		SerializedManySink<Object> sink = new SerializedManySink<>(
				new EmptyMany<>(),
				Context::empty
		);

		StepVerifier.create(sink.asFlux(), 0)
		            .expectSubscription()
		            .then(() -> {
			            assertThat(sink.tryEmitNext("boom"))
					            .as("emission")
					            .isEqualTo(Sinks.Emission.FAIL_OVERFLOW);
		            })
		            .then(sink::emitComplete)
		            .verifyComplete();
	}

	@Test
	public void shouldSignalErrorOnOverflow() {
		SerializedManySink<Object> sink = new SerializedManySink<>(
				new EmptyMany<>(),
				Context::empty
		);

		StepVerifier.create(sink.asFlux(), 0)
		            .expectSubscription()
		            .then(() -> sink.emitNext("boom"))
		            .verifyErrorMatches(Exceptions::isOverflow);
	}

	@Test
	public void shouldReturnTheEmission() {
		AtomicReference<SerializedManySink<Object>> sink = new AtomicReference<>();
		sink.set(
				new SerializedManySink<>(
						new EmptyMany<Object>() {
							@Override
							public Sinks.Emission tryEmitNext(Object o) {
								SerializedManySink.WIP.incrementAndGet(sink.get());
								return super.tryEmitNext(o);
							}
						},
						Context::empty
				)
		);

		assertThat(sink.get().tryEmitNext("boom"))
				.as("emission")
				.isEqualTo(Sinks.Emission.FAIL_OVERFLOW);
	}

	static class EmptyMany<T> implements Sinks.Many<T> {

		final Sinks.Many<T> delegate = Sinks.many().multicast().onBackpressureError();

		@Override
		public Sinks.Emission tryEmitNext(T o) {
			return Sinks.Emission.FAIL_OVERFLOW;
		}

		@Override
		public Sinks.Emission tryEmitComplete() {
			return delegate.tryEmitComplete();
		}

		@Override
		public Sinks.Emission tryEmitError(Throwable error) {
			return delegate.tryEmitError(error);
		}

		@Override
		public void emitNext(T o) {
			throw new IllegalStateException("Not expected to be called");
		}

		@Override
		public void emitComplete() {
			delegate.emitComplete();
		}

		@Override
		public void emitError(Throwable error) {
			delegate.emitError(error);
		}

		@Override
		public Flux<T> asFlux() {
			return delegate.asFlux();
		}
	}
}