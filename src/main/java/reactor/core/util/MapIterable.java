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

package reactor.core.util;

import java.util.Iterator;
import java.util.function.Function;

/**
 * Applies a mapper function to the elements of the source Iterable sequence.
 * 
 * @param <T> the source value type
 * @param <R> the result value type
 */
public final class MapIterable<T, R> implements Iterable<R> {

    final Iterable<? extends T> source;
    
    final Function<? super T, ? extends R> mapper;

    public MapIterable(Iterable<? extends T> source, Function<? super T, ? extends R> mapper) {
        this.source = source;
        this.mapper = mapper;
    }
    
    @Override
    public Iterator<R> iterator() {
        return new MapIterator<>(source.iterator(), mapper);
    }
    
    static final class MapIterator<T, R> implements Iterator<R> {
        final Iterator<? extends T> source;
        
        final Function<? super T, ? extends R> mapper;
        
        public MapIterator(Iterator<? extends T> source, Function<? super T, ? extends R> mapper) {
            this.source = source;
            this.mapper = mapper;
        }
        
        @Override
        public boolean hasNext() {
            return source.hasNext();
        }
        
        @Override
        public R next() {
            return mapper.apply(source.next());
        }
        
        @Override
        public void remove() {
            source.remove();
        }
    }
}
