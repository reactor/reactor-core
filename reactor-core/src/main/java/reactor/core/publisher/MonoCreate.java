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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;

import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.publisher.FluxCreate.SinkDisposable;
import javax.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.context.ContextRelay;

/**
 * Wraps a the downstream Subscriber into a single emission object
 * and calls the given callback to produce a signal (a)synchronously.
 * @param <T> the value type
 */
final class MonoCreate<T> extends Mono<T> {

    final Consumer<MonoSink<T>> callback;

    MonoCreate(Consumer<MonoSink<T>> callback) {
        this.callback = callback;
    }


    @Override
    public void subscribe(Subscriber<? super T> s, Context ctx) {
	    DefaultMonoSink<T> emitter = new DefaultMonoSink<>(s, ctx);

        s.onSubscribe(emitter);

        try {
            callback.accept(emitter);
        } catch (Throwable ex) {
            emitter.error(Operators.onOperatorError(ex));
        }
    }

	static final class DefaultMonoSink<T> extends AtomicBoolean
			implements MonoSink<T>, InnerProducer<T> {

		final Subscriber<? super T> actual;

		final Context context;

		volatile Disposable disposable;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<DefaultMonoSink, Disposable>
				DISPOSABLE =
				AtomicReferenceFieldUpdater.newUpdater(DefaultMonoSink.class, Disposable.class, "disposable");

        volatile int state;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<DefaultMonoSink> STATE =
                AtomicIntegerFieldUpdater.newUpdater(DefaultMonoSink.class, "state");

		volatile LongConsumer requestConsumer;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<DefaultMonoSink, LongConsumer> REQUEST_CONSUMER =
				AtomicReferenceFieldUpdater.newUpdater(DefaultMonoSink.class, LongConsumer.class, "requestConsumer");

        T value;

        static final int NO_REQUEST_HAS_VALUE = 1;
        static final int HAS_REQUEST_NO_VALUE = 2;
        static final int HAS_REQUEST_HAS_VALUE = 3;

		DefaultMonoSink(Subscriber<? super T> actual, Context ctx) {
			this.actual = actual;
			this.context = ctx;
		}

		@Override
		public MonoSink<T> contextualize(Function<Context, Context> doOnContext) {
			if (compareAndSet(false, true)) {
				Context c = doOnContext.apply(context);
				if(c != context) {
					ContextRelay.set(actual, c);
				}
			}
			return this;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == BooleanAttr.TERMINATED) return state == HAS_REQUEST_HAS_VALUE || state == NO_REQUEST_HAS_VALUE;
			if (key == BooleanAttr.CANCELLED) return Disposables.isDisposed(disposable);

			return InnerProducer.super.scanUnsafe(key);
		}

        @Override
        public void success() {
            if (STATE.getAndSet(this, HAS_REQUEST_HAS_VALUE) != HAS_REQUEST_HAS_VALUE) {
				try {
					actual.onComplete();
				}
				finally {
					disposeResource(false);
				}
            }
        }

        @Override
        public void success(@Nullable T value) {
            if (value == null) {
                success();
                return;
            }
            for (;;) {
                int s = state;
                if (s == HAS_REQUEST_HAS_VALUE || s == NO_REQUEST_HAS_VALUE) {
                    return;
                }
                if (s == HAS_REQUEST_NO_VALUE) {
                    if (STATE.compareAndSet(this, s, HAS_REQUEST_HAS_VALUE)) {
						try {
							actual.onNext(value);
							actual.onComplete();
						}
						finally {
							disposeResource(false);
						}
                    }
                    return;
                }
                this.value = value;
                if (STATE.compareAndSet(this, s, NO_REQUEST_HAS_VALUE)) {
                    return;
                }
            }
        }

        @Override
        public void error(Throwable e) {
            if (STATE.getAndSet(this, HAS_REQUEST_HAS_VALUE) != HAS_REQUEST_HAS_VALUE) {
				try {
					actual.onError(e);
				}
				finally {
					disposeResource(false);
				}
            } else {
                Operators.onErrorDropped(e);
            }
		}

		@Override
		public MonoSink<T> onRequest(LongConsumer consumer) {
			if (!REQUEST_CONSUMER.compareAndSet(this, null, consumer)) {
				throw new IllegalStateException("A consumer has already been assigned to consume requests");
			}
			return this;
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		@Override
		public MonoSink<T> onCancel(Disposable d) {
			//noinspection ConstantConditions
			if (d != null) {
				SinkDisposable sd = new SinkDisposable(null, d);
				if (!DISPOSABLE.compareAndSet(this, null, sd)) {
					Disposable c = disposable;
					if (c instanceof SinkDisposable) {
						SinkDisposable current = (SinkDisposable) c;
						if (current.onCancel == null) current.onCancel = d;
						else d.dispose();
					}
				}
			}
			return this;
		}

		@Override
		public MonoSink<T> onDispose(Disposable d) {
			//noinspection ConstantConditions
			if (d != null) {
				SinkDisposable sd = new SinkDisposable(d, null);
				if (!DISPOSABLE.compareAndSet(this, null, sd)) {
					Disposable c = disposable;
					if (c instanceof SinkDisposable) {
						SinkDisposable current = (SinkDisposable) c;
						if (current.disposable == null)
							current.disposable = d;
						else
							d.dispose();
					}
				}
			}
			return this;
		}

        @Override
        public void request(long n) {
            if (Operators.validate(n)) {
				LongConsumer consumer = requestConsumer;
				if (consumer != null) {
					consumer.accept(n);
				}
                for (;;) {
                    int s = state;
                    if (s == HAS_REQUEST_NO_VALUE || s == HAS_REQUEST_HAS_VALUE) {
                        return;
                    }
                    if (s == NO_REQUEST_HAS_VALUE) {
                        if (STATE.compareAndSet(this, s, HAS_REQUEST_HAS_VALUE)) {
							try {
								actual.onNext(value);
								actual.onComplete();
							}
							finally {
								disposeResource(false);
							}
                        }
                        return;
                    }
                    if (STATE.compareAndSet(this, s, HAS_REQUEST_NO_VALUE)) {
                        return;
                    }
                }
            }
        }

        @Override
        public void cancel() {
            if (STATE.getAndSet(this, HAS_REQUEST_HAS_VALUE) != HAS_REQUEST_HAS_VALUE) {
                value = null;
                disposeResource(true);
            }
        }

		void disposeResource(boolean isCancel) {
			Disposable d = disposable;
			if (d != Disposables.DISPOSED) {
				d = DISPOSABLE.getAndSet(this, Disposables.DISPOSED);
				if (d != null && d != Disposables.DISPOSED) {
					if (isCancel && d instanceof SinkDisposable) {
						((SinkDisposable) d).cancel();
					}
					d.dispose();
				}
			}
		}

    }
}
