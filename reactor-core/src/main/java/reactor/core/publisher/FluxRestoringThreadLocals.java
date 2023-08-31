/*
 * Copyright (c) 2016-2023 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import io.micrometer.context.ContextSnapshot;
import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.util.context.Context;

final class FluxRestoringThreadLocals<T> extends Flux<T> implements SourceProducer<T> {

	private final Publisher<? extends T> source;

	FluxRestoringThreadLocals(Publisher<? extends T> source) {
		this.source = source;
	}

	@SuppressWarnings("try")
	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Context c = actual.currentContext();
		try (ContextSnapshot.Scope ignored = ContextPropagation.setThreadLocals(c)) {
			source.subscribe(new FluxContextWriteRestoringThreadLocals.ContextWriteRestoringThreadLocalsSubscriber<>(actual, c));
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return SourceProducer.super.scanUnsafe(key);
	}
}
