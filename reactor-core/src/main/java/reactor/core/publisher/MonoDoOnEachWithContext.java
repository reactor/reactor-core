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

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import reactor.core.CoreSubscriber;
import reactor.util.context.Context;

/**
 * @author Simon Baslé
 */
final class MonoDoOnEachWithContext<T> extends MonoOperator<T, T> {

	final BiConsumer<? super Signal<T>, ? super Context> onSignalAndContext;

	MonoDoOnEachWithContext(Mono<? extends T> source,
			BiConsumer<? super Signal<T>, ? super Context> onSignalAndContext) {
		super(source);
		this.onSignalAndContext = Objects.requireNonNull(onSignalAndContext, "onSignalAndContext");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		//TODO fuseable version?
		//TODO conditional version?
		source.subscribe(new FluxDoOnEach.DoOnEachSubscriber<>(actual, onSignalAndContext));
	}
}
