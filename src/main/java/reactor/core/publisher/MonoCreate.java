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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import reactor.core.Cancellation;
import reactor.core.Disposable;
import reactor.core.publisher.FluxCreate.SinkDisposable;



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
    public void subscribe(Subscriber<? super T> s) {
	    DefaultMonoSink<T> emitter = new DefaultMonoSink<>(s);

        s.onSubscribe(emitter);

        try {
            callback.accept(emitter);
        } catch (Throwable ex) {
            emitter.error(Operators.onOperatorError(ex));
        }
    }

	static final class DefaultMonoSink<T>
			implements MonoSink<T>, InnerProducer<T> {

		final Subscriber<? super T> actual;

		volatile Disposable disposable;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<DefaultMonoSink, Disposable>
				DISPOSABLE =
				AtomicReferenceFieldUpdater.newUpdater(DefaultMonoSink.class, Disposable.class, "disposable");

        volatile int state;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<DefaultMonoSink> STATE =
                AtomicIntegerFieldUpdater.newUpdater(DefaultMonoSink.class, "state");

        T value;

        static final int NO_REQUEST_HAS_VALUE = 1;
        static final int HAS_REQUEST_NO_VALUE = 2;
        static final int HAS_REQUEST_HAS_VALUE = 3;

		DefaultMonoSink(Subscriber<? super T> actual) {
			this.actual = actual;

		}

		@Override
		public Object scan(Attr key) {
			switch (key){
				case TERMINATED:
					return state == HAS_REQUEST_HAS_VALUE || state == NO_REQUEST_HAS_VALUE;
				case CANCELLED:
					return disposable == Flux.CANCELLED;
			}
			return InnerProducer.super.scan(key);
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
        public void success(T value) {
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
		public Subscriber<? super T> actual() {
			return actual;
		}

		@Override
		public MonoSink<T> onCancel(Disposable d) {
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

		@Deprecated
		@Override
		public MonoSink<T> setCancellation(Cancellation c) {
			return onCancel(new Disposable() {
				@Override
				public void dispose() {
					c.dispose();
				}
			});
		}

        @Override
        public void request(long n) {
            if (Operators.validate(n)) {
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
			if (d != Flux.CANCELLED) {
				d = DISPOSABLE.getAndSet(this, Flux.CANCELLED);
				if (d != null && d != Flux.CANCELLED) {
					if (isCancel && d instanceof SinkDisposable) {
						((SinkDisposable) d).cancel();
					}
					d.dispose();
				}
			}
		}

    }
}
