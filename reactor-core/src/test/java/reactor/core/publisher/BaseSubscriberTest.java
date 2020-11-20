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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.Disposable;
import reactor.test.util.LoggerUtils;
import reactor.test.util.TestLogger;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class BaseSubscriberTest {

	@Test
	public void partialRequestAndCancel() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);
		AtomicInteger lastValue = new AtomicInteger(0);

		Flux<Integer> intFlux = Flux.range(1, 1000);
		intFlux.subscribe(new BaseSubscriber<Integer>() {

			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				request(1);
			}

			@Override
			public void hookOnNext(Integer integer) {
				assertThat(lastValue.compareAndSet(integer - 1, integer)).as("compareAndSet of %d", integer).isTrue();
				if (integer < 10) {
					request(1);
				}
				else {
					cancel();
				}
			}

			@Override
			protected void hookOnComplete() {
				fail("expected cancellation, not completion");
			}

			@Override
			protected void hookOnError(Throwable throwable) {
				fail("expected cancellation, not error " + throwable);
			}

			@Override
			protected void hookFinally(SignalType type) {
				latch.countDown();
				assertThat(type).isEqualTo(SignalType.CANCEL);
			}
		});

		latch.await(500, TimeUnit.MILLISECONDS);
		assertThat(lastValue).hasValue(10);
	}

	@Test
	public void onErrorCallbackNotImplemented() {
		TestLogger testLogger = new TestLogger();
		LoggerUtils.enableCaptureWith(testLogger);
		try {
			Flux<String> flux = Flux.error(new IllegalStateException());

			flux.subscribe(new BaseSubscriber<String>() {
				@Override
				protected void hookOnSubscribe(Subscription subscription) {
					request(1);
				}

				@Override
				protected void hookOnNext(String value) {
					//NO-OP
				}
			});
			Assertions.assertThat(testLogger.getErrContent())
			          .contains("Operator called default onErrorDropped")
			          .contains("reactor.core.Exceptions$ErrorCallbackNotImplemented: java.lang.IllegalStateException");
		}
		finally {
			LoggerUtils.disableCapture();
		}
	}

	@Test
	public void onSubscribeErrorPropagatedToOnError() {
		Flux<String> flux = Flux.just("foo");
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<SignalType> checkFinally = new AtomicReference<>();

		flux.subscribe(new BaseSubscriber<String>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				throw new IllegalStateException("boom");
			}

			@Override
			protected void hookOnNext(String value) {
				//NO-OP
			}

			@Override
			protected void hookOnError(Throwable throwable) {
				error.set(throwable);
			}

			@Override
			protected void hookFinally(SignalType type) {
				checkFinally.set(type);
			}
		});
		assertThat(checkFinally).hasValue(SignalType.ON_ERROR);
		assertThat(error.get()).isInstanceOf(IllegalStateException.class);
	}

	@Test
	public void onSubscribeFatalThrown() {
		Flux<String> flux = Flux.just("foo");
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<SignalType> checkFinally = new AtomicReference<>();

		assertThatExceptionOfType(OutOfMemoryError.class).isThrownBy(() -> {
			flux.subscribe(new BaseSubscriber<String>() {
				@Override
				protected void hookOnSubscribe(Subscription subscription) {
					throw new OutOfMemoryError("boom");
				}

				@Override
				protected void hookOnNext(String value) {
					//NO-OP
				}

				@Override
				protected void hookOnError(Throwable throwable) {
					error.set(throwable);
				}

				@Override
				protected void hookFinally(SignalType type) {
					checkFinally.set(type);
				}
			});
		});
		Assertions.assertThat(checkFinally.get()).isNull();
		Assertions.assertThat(error.get()).isNull();
	}

	@Test
	public void onNextErrorPropagatedToOnError() {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<SignalType> checkFinally = new AtomicReference<>();
		Flux<String> flux = Flux.just("foo");

		flux.subscribe(new BaseSubscriber<String>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				requestUnbounded();
			}

			@Override
			protected void hookOnNext(String value) {
				throw new IllegalArgumentException("boom");
			}

			@Override
			protected void hookOnError(Throwable throwable) {
				error.set(throwable);
			}

			@Override
			protected void hookFinally(SignalType type) {
				checkFinally.set(type);
			}
		});
		assertThat(checkFinally).hasValue(SignalType.ON_ERROR);
		assertThat(error.get()).isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	public void onCompleteErrorPropagatedToOnError() {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<SignalType> checkFinally = new AtomicReference<>();
		Flux<String> flux = Flux.just("foo");

		flux.subscribe(new BaseSubscriber<String>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				requestUnbounded();
			}

			@Override
			protected void hookOnNext(String value) {
				//NO-OP
			}

			@Override
			protected void hookOnComplete() {
				throw new IllegalArgumentException("boom");
			}

			@Override
			protected void hookOnError(Throwable throwable) {
				error.set(throwable);
			}

			@Override
			protected void hookFinally(SignalType type) {
				checkFinally.set(type);
			}
		});
		assertThat(checkFinally).hasValue(SignalType.ON_COMPLETE);
		assertThat(error.get()).isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	public void finallyExecutesWhenHookOnCompleteFails() {
		RuntimeException err = new IllegalArgumentException("hookOnComplete");
		AtomicReference<SignalType> checkFinally = new AtomicReference<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Flux.just("foo")
		    .subscribe(new BaseSubscriber<String>() {
			    @Override
			    protected void hookOnSubscribe(Subscription subscription) {
			    	requestUnbounded();
			    }

			    @Override
			    protected void hookOnNext(String value) {
			    }

			    @Override
			    protected void hookOnError(Throwable throwable) {
				    error.set(throwable);
			    }

			    @Override
			    protected void hookOnComplete() {
				    throw err;
			    }

			    @Override
			    protected void hookFinally(SignalType type) {
				    checkFinally.set(type);
			    }
		    });

		assertThat(checkFinally).hasValue(SignalType.ON_COMPLETE);
		assertThat(error).hasValue(err);
	}

	@Test
	public void finallyExecutesWhenHookOnErrorFails() {
		TestLogger testLogger = new TestLogger();
		LoggerUtils.enableCaptureWith(testLogger);
		try {
			RuntimeException error = new IllegalArgumentException("hookOnError");
			AtomicReference<SignalType> checkFinally = new AtomicReference<>();

			Flux.<String>error(new IllegalStateException("someError")).subscribe(new BaseSubscriber<String>() {
				@Override
				protected void hookOnSubscribe(Subscription subscription) {
					requestUnbounded();
				}

				@Override
				protected void hookOnNext(String value) {
				}

				@Override
				protected void hookOnError(Throwable throwable) {
					throw error;
				}

				@Override
				protected void hookFinally(SignalType type) {
					checkFinally.set(type);
				}
			});
			Assertions.assertThat(testLogger.getErrContent())
			          .contains("Operator called default onErrorDropped")
			          .contains(error.getMessage());
			assertThat(checkFinally).hasValue(SignalType.ON_ERROR);
		}
		finally {
			LoggerUtils.disableCapture();
		}
	}

	@Test
	public void finallyExecutesWhenHookOnCancelFails() {
		RuntimeException err = new IllegalArgumentException("hookOnCancel");
		AtomicReference<SignalType> checkFinally = new AtomicReference<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Flux.just("foo")
		    .subscribe(new BaseSubscriber<String>() {
			    @Override
			    protected void hookOnSubscribe(Subscription subscription) {
				    this.cancel();
			    }

			    @Override
			    protected void hookOnNext(String value) {
			    }

			    @Override
			    protected void hookOnError(Throwable throwable) {
				    error.set(throwable);
			    }

			    @Override
			    protected void hookOnCancel() {
				    throw err;
			    }

			    @Override
			    protected void hookFinally(SignalType type) {
				    checkFinally.set(type);
			    }
		    });

		assertThat(checkFinally).hasValue(SignalType.CANCEL);
		assertThat(error).hasValue(err);
	}

	@Test
	public void disposeCancels() throws InterruptedException {
		AtomicReference<SignalType> onFinally = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(1);

		BaseSubscriber<Long> sub = new BaseSubscriber<Long>() {
			@Override
			protected void hookOnSubscribe(
					Subscription subscription) {
				requestUnbounded();
			}

			@Override
			protected void hookOnNext(Long value) {
				fail("delay was not cancelled");
			}

			@Override
			protected void hookFinally(SignalType type) {
				onFinally.set(type);
				latch.countDown();
			}
		};

		Disposable d = Mono.delay(Duration.ofSeconds(1))
		                   .subscribeWith(sub);
		d.dispose();

		assertThat(latch.await(1500, TimeUnit.MILLISECONDS)).as("delay should be skipped by cancel").isTrue();
		assertThat(onFinally).hasValue(SignalType.CANCEL);
	}
}
