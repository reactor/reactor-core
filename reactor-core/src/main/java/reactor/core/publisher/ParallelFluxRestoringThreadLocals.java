/*
 * Copyright (c) 2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;

class ParallelFluxRestoringThreadLocals<T> extends ParallelFlux<T> implements
                                                                          Scannable {

	private final ParallelFlux<? extends T> source;

	ParallelFluxRestoringThreadLocals(ParallelFlux<? extends T> source) {
		this.source = source;
	}

	@Override
	public int parallelism() {
		return source.parallelism();
	}

	@Override
	public void subscribe(CoreSubscriber<? super T>[] subscribers) {
		CoreSubscriber<? super T>[] actualSubscribers =
				Operators.restoreContextOnSubscribers(source, subscribers);

		source.subscribe(actualSubscribers);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		if (key == InternalProducerAttr.INSTANCE) return true;
		return null;
	}
}
