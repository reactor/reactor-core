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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import reactor.core.Exceptions;
import reactor.core.publisher.Sinks.Emission;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;

public class SerializedManySinkTest {

	@Rule
	public Timeout timeout = new Timeout(5, TimeUnit.SECONDS);

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
					            .isEqualTo(Emission.FAIL_OVERFLOW);
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
							public Emission tryEmitNext(Object o) {
								SerializedManySink.LOCKED_AT.set(sink.get(), new Thread());
								return super.tryEmitNext(o);
							}
						},
						Context::empty
				)
		);

		assertThat(sink.get().tryEmitNext("boom"))
				.as("emission")
				.isEqualTo(Emission.FAIL_OVERFLOW);
	}

	@Test
	public void sameThreadRecursion() throws Exception {
		Sinks.Many<Object> sink = Sinks.many().unsafe().multicast().directBestEffort();
		SerializedManySink<Object> manySink = new SerializedManySink<>(sink, Context::empty);

		CompletableFuture<Object> muchFuture = sink.asFlux().doOnNext(o -> {
			if ("first".equals(o)) {
				assertThat(manySink.lockedAt).as("lockedAt").isEqualTo(Thread.currentThread());
				assertThat(manySink.tryEmitNext("second")).as("second").isEqualTo(Emission.OK);
			}
		}).next().toFuture();

		assertThat(manySink.tryEmitNext("first")).as("first").isEqualTo(Emission.OK);
		muchFuture.get(5, TimeUnit.SECONDS);
	}

	static class EmptyMany<T> implements Sinks.Many<T> {

		final Sinks.Many<T> delegate = Sinks.many().unsafe().multicast().directBestEffort();

		@Override
		public Emission tryEmitNext(T o) {
			return Emission.FAIL_OVERFLOW;
		}

		@Override
		public Emission tryEmitComplete() {
			return delegate.tryEmitComplete();
		}

		@Override
		public Emission tryEmitError(Throwable error) {
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
		public int currentSubscriberCount() {
			return delegate.currentSubscriberCount();
		}

		@Override
		public Flux<T> asFlux() {
			return delegate.asFlux();
		}
	}
}