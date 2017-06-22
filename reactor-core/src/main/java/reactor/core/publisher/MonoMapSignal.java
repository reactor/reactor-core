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

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.util.context.Context;

/**
 * Maps the values of the source publisher one-on-one via a mapper function.
 *
 * @param <T> the source value type
 * @param <R> the result value type
 * @author Stephane Maldini
 */
final class MonoMapSignal<T, R> extends MonoOperator<T, R> {

    final Function<? super T, ? extends R> mapperNext;
    final Function<? super Throwable, ? extends R> mapperError;
    final Supplier<? extends R>            mapperComplete;

    /**
     * Constructs a FluxMapSignal instance with the given source and mappers.
     *
     * @param source the source Publisher instance
     * @param mapperNext the next mapper function
     * @param mapperError the error mapper function
     * @param mapperComplete the complete mapper function
     *
     * @throws NullPointerException if either {@code source} is null or all {@code mapper} are null.
     */
    MonoMapSignal(Mono<? extends T> source,
		    @Nullable Function<? super T, ? extends R> mapperNext,
		    @Nullable Function<? super Throwable, ? extends R> mapperError,
		    @Nullable Supplier<? extends R> mapperComplete) {
        super(source);
	    if(mapperNext == null && mapperError == null && mapperComplete == null){
		    throw new IllegalArgumentException("Map Signal needs at least one valid mapper");
	    }

        this.mapperNext = mapperNext;
        this.mapperError = mapperError;
        this.mapperComplete = mapperComplete;
    }

    @Override
    public void subscribe(Subscriber<? super R> s, Context ctx) {
        source.subscribe(new FluxMapSignal.FluxMapSignalSubscriber<>(s, mapperNext, mapperError, mapperComplete), ctx);
    }
}