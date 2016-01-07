/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.Exceptions;
import reactor.core.support.BackpressureUtils;
import reactor.fn.Function;

/**
 * Maps the values of the source publisher one-on-one via a mapper function.
 *
 * @param <T> the source value type
 * @param <R> the result value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class FluxMap<T, R> extends reactor.Flux.FluxBarrier<T, R> {

    final Function<? super T, ? extends R> mapper;

    /**
     * Constructs a FluxMap instance with the given source and mapper.
     *
     * @param source the source Publisher instance
     * @param mapper the mapper function
     *
     * @throws NullPointerException if either {@code source} or {@code mapper} is null.
     */
    public FluxMap(Publisher<? extends T> source, Function<? super T, ? extends R> mapper) {
        super(source);
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    public Function<? super T, ? extends R> mapper() {
        return mapper;
    }

    @Override
    public void subscribe(Subscriber<? super R> s) {
        source.subscribe(new FluxMapSubscriber<>(s, mapper));
    }

    static final class FluxMapSubscriber<T, R>
            implements Subscriber<T>, Upstream, Downstream, FeedbackLoop, ActiveUpstream {

        final Subscriber<? super R>            actual;
        final Function<? super T, ? extends R> mapper;

        boolean done;

        Subscription s;

        public FluxMapSubscriber(Subscriber<? super R> actual, Function<? super T, ? extends R> mapper) {
            this.actual = actual;
            this.mapper = mapper;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (BackpressureUtils.validate(this.s, s)) {
                this.s = s;

                actual.onSubscribe(s);
            }
        }

        @Override
        public void onNext(T t) {
            if (done) {
                Exceptions.onNextDropped(t);
                return;
            }

            R v;

            try {
                v = mapper.apply(t);
            }
            catch (Throwable e) {
                done = true;
                s.cancel();
                actual.onError(e);
                return;
            }

            if (v == null) {
                done = true;
                s.cancel();
                actual.onError(new NullPointerException("The mapper returned a null value."));
                return;
            }

            actual.onNext(v);
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                Exceptions.onErrorDropped(t);
                return;
            }

            done = true;

            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;

            actual.onComplete();
        }

        @Override
        public boolean isStarted() {
            return s != null && !done;
        }

        @Override
        public boolean isTerminated() {
            return done;
        }

        @Override
        public Object downstream() {
            return actual;
        }

        @Override
        public Object delegateInput() {
            return mapper;
        }

        @Override
        public Object delegateOutput() {
            return null;
        }

        @Override
        public Object upstream() {
            return s;
        }
    }
}