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
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.context.Context;

/**
 * Materialize current {@link Context} from the subscribing flow
 */
@Deprecated
final class MonoCurrentContext extends Mono<Context>
		implements Fuseable, Scannable {

	static final MonoCurrentContext INSTANCE = new MonoCurrentContext();

	@SuppressWarnings("unchecked")
	public void subscribe(CoreSubscriber<? super Context> actual) {
		Context ctx = actual.currentContext();
		actual.onSubscribe(Operators.scalarSubscription(actual, ctx));
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}
}
