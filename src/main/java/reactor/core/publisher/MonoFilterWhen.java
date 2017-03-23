/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.util.concurrent.QueueSupplier;

/**
 * @author Simon Baslé
 */
public class MonoFilterWhen<T> extends MonoSource<T, T> {

	final Function<? super T, ? extends Publisher<Boolean>> asyncPredicate;

	MonoFilterWhen(Publisher<T> source,
			Function<? super T, ? extends Publisher<Boolean>> asyncPredicate,
			int bufferSize) {
		super(source);
		this.asyncPredicate = asyncPredicate;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new FluxFilterWhen.FilterWhenSubscriber<>(s,
				asyncPredicate, QueueSupplier.XS_BUFFER_SIZE));
	}
}
