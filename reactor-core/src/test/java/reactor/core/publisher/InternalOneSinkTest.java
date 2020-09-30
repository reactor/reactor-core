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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import reactor.core.Exceptions;
import reactor.core.publisher.Sinks.Emission;
import reactor.core.publisher.Sinks.EmissionException;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assumptions.assumeThat;

class InternalOneSinkTest {

	@ParameterizedTest
	@EnumSource(value = Emission.class)
	void shouldDelegateToHandler(Emission emission) {
		assumeThat(emission.hasFailed()).isTrue();
		Sinks.One<Object> sink = new InternalOneSinkAdapter<Object>() {
			@Override
			public Emission tryEmitValue(Object value) {
				return emission;
			}

			@Override
			public Emission tryEmitError(Throwable error) {
				return emission;
			}

			@Override
			public Emission tryEmitEmpty() {
				return emission;
			}
		};

		for (SignalType signalType : new SignalType[] {SignalType.ON_NEXT, SignalType.ON_ERROR, SignalType.ON_COMPLETE}) {
			AtomicReference<Emission> emissionRef = new AtomicReference<>();
			try {
				EmitFailureHandler handler = (failedSignalType, failedEmission) -> {
					emissionRef.set(failedEmission);
					return false;
				};
				switch (signalType) {
					case ON_NEXT:
						sink.emitValue("Hello", handler);
						break;
					case ON_ERROR:
						sink.emitError(new Exception("boom"), handler);
						break;
					case ON_COMPLETE:
						sink.emitEmpty(handler);
						break;
					default:
						throw new IllegalStateException();
				}
			}
			catch (EmissionException e) {
				assertThat(e.getReason()).isEqualTo(Emission.FAIL_NON_SERIALIZED);
			}
			assertThat(emissionRef).as("emission").hasValue(emission);
		}
	}

	@Test
	void shouldRetry() {
		AtomicReference<Emission> nextEmission = new AtomicReference<>(Emission.FAIL_NON_SERIALIZED);

		Sinks.One<Object> sink = new InternalOneSinkAdapter<Object>() {

			@Override
			public Emission tryEmitValue(Object value) {
				return nextEmission.get();
			}

			@Override
			public Emission tryEmitEmpty() {
				throw new IllegalStateException();
			}

			@Override
			public Emission tryEmitError(Throwable error) {
				throw new IllegalStateException();
			}
		};

		assertThatExceptionOfType(EmissionException.class).isThrownBy(() -> {
			sink.emitValue("Hello", (signalType, emission) -> {
				nextEmission.set(Emission.OK);
				return false;
			});
		});

		nextEmission.set(Emission.FAIL_NON_SERIALIZED);
		sink.emitValue("Hello", (signalType, emission) -> {
			nextEmission.set(Emission.OK);
			return true;
		});
	}


	@Test
	void shouldRethrowNonSerializedEmission() {
		Sinks.One<Object> sink = new InternalOneSinkAdapter<Object>() {
			@Override
			public Emission tryEmitValue(Object o) {
				return Emission.FAIL_NON_SERIALIZED;
			}
		};

		assertThatExceptionOfType(EmissionException.class).isThrownBy(() -> {
			sink.emitValue("Hello", EmitFailureHandler.FAIL_FAST);
		});
	}

	@Test
	void shouldFailOnOverflow() {
		AtomicReference<Throwable> errorRef = new AtomicReference<>();
		Sinks.One<Object> sink = new InternalOneSinkAdapter<Object>() {
			@Override
			public Emission tryEmitValue(Object o) {
				return Emission.FAIL_OVERFLOW;
			}

			@Override
			public Emission tryEmitError(Throwable error) {
				errorRef.set(error);
				return Emission.OK;
			}
		};

		sink.emitValue("Hello", EmitFailureHandler.FAIL_FAST);
		assertThat(errorRef.get()).as("error").matches(Exceptions::isOverflow);
	}

	@Test
	void shouldRetryUntilExitCondition() {
		AtomicInteger i = new AtomicInteger();
		Sinks.One<Object> sink = new InternalOneSinkAdapter<Object>() {

			@Override
			public Emission tryEmitValue(Object o) {
				return i.incrementAndGet() == 5 ? Emission.OK : Emission.FAIL_NON_SERIALIZED;
			}

			@Override
			public Emission tryEmitEmpty() {
				throw new IllegalStateException();
			}

			@Override
			public Emission tryEmitError(Throwable error) {
				throw new IllegalStateException();
			}
		};

		sink.emitValue("Hello", (signalType, emission) -> true);
		assertThat(i).hasValue(5);
	}

	static class InternalOneSinkAdapter<T> implements InternalOneSink<T> {

		@Override
		public Context currentContext() {
			return Context.empty();
		}

		@Override
		public Object scanUnsafe(Attr key) {
			return null;
		}

		@Override
		public Emission tryEmitValue(T value) {
			return Emission.OK;
		}

		@Override
		public Emission tryEmitError(Throwable error) {
			return Emission.OK;
		}

		@Override
		public Emission tryEmitEmpty() {
			return Emission.OK;
		}

		@Override
		public int currentSubscriberCount() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Mono<T> asMono() {
			throw new UnsupportedOperationException();
		}
	}
}