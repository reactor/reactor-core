/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import reactor.core.CoreSubscriber;

/**
 * @author Stephane Maldini
 */
final class MonoDematerialize<T> extends InternalMonoOperator<Signal<T>, T> {

	MonoDematerialize(Mono<Signal<T>> source) {
		super(source);
	}

	@Override
	public CoreSubscriber<? super Signal<T>> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new FluxDematerialize.DematerializeSubscriber<>(actual, true);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}
}
