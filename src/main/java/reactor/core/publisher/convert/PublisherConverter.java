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

package reactor.core.publisher.convert;

import org.reactivestreams.Publisher;
import reactor.Flux;
import reactor.fn.BiFunction;
import reactor.fn.Function;
import reactor.fn.Predicate;
import reactor.fn.Supplier;

/**
 * @author Stephane Maldini
 */
public abstract class PublisherConverter<TYPE>
		implements Function<Object, Publisher<?>>, BiFunction<Publisher<?>, Class<?>, TYPE>, Predicate<Object>,
		           Supplier<Class<TYPE>> {

	abstract protected Publisher<?> toPublisher(Object o);

	abstract protected TYPE fromPublisher(Publisher<?> source);

	@Override
	public final TYPE apply(Publisher<?> source, Class<?> to) {
		if (get().isAssignableFrom(to)) {
			TYPE t = fromPublisher(source);

			if (t == null) {
				throw new IllegalArgumentException("Cannot convert " + source + " source to " + to + " type");
			}
			return t;
		}
		return null;
	}

	@Override
	public Publisher<?> apply(Object o) {
		Publisher<?> p = toPublisher(o);
		if (p == null) {
			throw new IllegalArgumentException("Cannot convert " + o + " source to Publisher type");
		}
		return p;
	}

	@Override
	public boolean test(Object o) {
		return get().isAssignableFrom(o.getClass());
	}
}
