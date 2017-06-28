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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.hamcrest.Matchers;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.util.context.Context;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

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
				assertTrue("unexpected previous value for " + integer,
						lastValue.compareAndSet(integer - 1, integer));
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
				assertThat(type, is(SignalType.CANCEL));
			}
		});

		latch.await(500, TimeUnit.MILLISECONDS);
		assertThat(lastValue.get(), is(10));
	}

	@Test
	public void contextPassing() throws InterruptedException {
		AtomicReference<Context> c = new AtomicReference<>();
		AtomicReference<Context> innerC = new AtomicReference<>();

		Flux.range(1, 1000)
		    //old: test=baseSubscriber
		    //next: empty
		    //return: test=baseSubscriber_range
		    .contextMap((old, next) -> next.put("test", old.get("test") + "_range"))
		    .log()
		    .flatMapSequential(d -> Mono.just(d)
		                      //old: test=baseSubscriber_range_take
		                      //next: test=baseSubscriber_range_innerFlatmap
		                      //return: old (discarded since inner)
		                      .contextMap((old, next) -> {
			                      if (innerC.get() == null) {
				                      innerC.set(next.put("test", old.get("test") + "_innerFlatmap"));
			                      }
			                      return old;
		                      })
		                      .log())
		    .map(d -> d)
		    .take(10)
		    //old: test=baseSubscriber
		    //next: test=baseSubscriber_range
		    //return: test=baseSubscriber_range_take
		    .contextMap((old, next) -> next.put("test", next.get("test") + "_take"))
		    .log()
		    .subscribe(new BaseSubscriber<Integer>() {
				@Override
				public Context currentContext() {
					return Context.empty()
					              .put("test", "baseSubscriber");
				}

				@Override
				public void onContextUpdate(Context context) {
					c.set(context);
				}
		});

		assertThat(c.get()
		            .get("test"), is("baseSubscriber_range_take"));

		assertThat(c.get()
		            .get("test2"), Matchers.nullValue());

		assertThat(innerC.get()
		                 .get("test"), is("baseSubscriber_range_take_innerFlatmap"));
	}

	@Test
	public void contextPassing2() throws InterruptedException {
		AtomicReference<String> innerC = new AtomicReference<>();

		Flux.range(1, 1000)
		    .contextMap((old, next) -> next.put("test", "foo"))
		    .log()
		    .flatMapSequential(d ->
				    Mono.just(d)
				        .contextMap((old, next) -> {
					        if (innerC.get() == null) {
						        innerC.set(""+ old.get("test") + old.get("test2"));
					        }
					        return old;
				        })
				        .log())
		    .map(d -> d)
		    .take(10)
		    .contextMap((old, next) -> next.put("test2", "bar"))
		    .log()
		    .subscribe();

		assertThat(innerC.get(), is("foobar"));
	}

	@Test
	public void onErrorCallbackNotImplemented() {
		Flux<String> flux = Flux.error(new IllegalStateException());

		try {
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
			fail("expected UnsupportedOperationException");
		}
		catch (UnsupportedOperationException e) {
			assertThat(e.getClass()
			            .getSimpleName(), is("ErrorCallbackNotImplemented"));
			assertThat(e.getCause(), is(instanceOf(IllegalStateException.class)));
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
		assertThat(checkFinally.get(), is(SignalType.ON_ERROR));
		assertThat(error.get(), is(instanceOf(IllegalStateException.class)));
	}

	@Test(expected = OutOfMemoryError.class)
	public void onSubscribeFatalThrown() {
		Flux<String> flux = Flux.just("foo");
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<SignalType> checkFinally = new AtomicReference<>();

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
		assertThat(checkFinally.get(), is(SignalType.ON_ERROR));
		assertThat(error.get(), is(nullValue()));
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
		assertThat(checkFinally.get(), is(SignalType.ON_ERROR));
		assertThat(error.get(), is(instanceOf(IllegalArgumentException.class)));
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
		assertThat(checkFinally.get(), is(SignalType.ON_COMPLETE));
		assertThat(error.get(), is(instanceOf(IllegalArgumentException.class)));
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

		assertThat(checkFinally.get(), is(SignalType.ON_COMPLETE));
		assertThat(error.get(), is(err));
	}

	@Test
	public void finallyExecutesWhenHookOnErrorFails() {
		RuntimeException err = new IllegalArgumentException("hookOnError");
		AtomicReference<SignalType> checkFinally = new AtomicReference<>();

		try {
			Flux.<String>error(new IllegalStateException("someError"))
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
					throw err;
				}

				@Override
				protected void hookFinally(SignalType type) {
					checkFinally.set(type);
				}
			});
			fail("expected " + err);
		}
		catch (Throwable e) {
			assertThat(Exceptions.unwrap(e), is(err));
		}
		assertThat(checkFinally.get(), is(SignalType.ON_ERROR));
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

		assertThat(checkFinally.get(), is(SignalType.CANCEL));
		assertThat(error.get(), is(err));
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

		assertTrue("delay not skipped by cancel", latch.await(1500, TimeUnit.MILLISECONDS));
		assertThat(onFinally.get(), is(SignalType.CANCEL));
	}
}