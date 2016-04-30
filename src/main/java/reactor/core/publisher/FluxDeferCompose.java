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
import java.util.function.Function;

import org.reactivestreams.*;

import reactor.core.util.*;

/**
 * Defers the composition of operators to subscription time.
 * <p>
 * This allows per-subscriber state to be present while also
 * composing operators with the upstream.
 * 
 * @param <T> the input stream type
 * @param <R> the output stream type
 */
public final class FluxDeferCompose<T, R> extends FluxSource<T, R> {

    final Function<? super Flux<? extends T>, ? extends Publisher<? extends R>> composer;

    public FluxDeferCompose(Flux<? extends T> source,
            Function<? super Flux<? extends T>, ? extends Publisher<? extends R>> composer) {
        super(source);
        this.composer = Objects.requireNonNull(composer, "composer");
    }
    
    @Override
    public void subscribe(Subscriber<? super R> s) {
        
        Publisher<? extends R> output;
        
        try {
            output = composer.apply((Flux<? extends T>)source);
        } catch (Throwable ex) {
            Exceptions.throwIfFatal(ex);
            EmptySubscription.error(s, ex);
            return;
        }
        
        if (output == null) {
            EmptySubscription.error(s, new NullPointerException("The composer returned a null Publisher"));
            return;
        }
        
        output.subscribe(s);
    }
}
