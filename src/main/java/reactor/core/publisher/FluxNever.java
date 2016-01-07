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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.ReactiveState;

/**
 * Represents an never publisher which only calls onSubscribe.
 * <p>
 * This Publisher is effectively stateless and only a single instance exists.
 * Use the {@link #instance()} method to obtain a properly type-parametrized view of it.
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class FluxNever extends reactor.Flux<Object>
        implements ReactiveState.Factory, ReactiveState.ActiveUpstream {

    private static final Publisher<Object> INSTANCE = new FluxNever();

    private FluxNever() {
        // deliberately no op
    }

    @Override
    public boolean isStarted() {
        return true;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public void subscribe(Subscriber<? super Object> s) {
        s.onSubscribe(EmptySubscription.INSTANCE);
    }

    /**
     * Returns a properly parametrized instance of this never Publisher.
     *
     * @param <T> the value type
     * @return a properly parametrized instance of this never Publisher
     */
    @SuppressWarnings("unchecked")
    public static <T> reactor.Flux<T> instance() {
        return (reactor.Flux<T>) INSTANCE;
    }
}
