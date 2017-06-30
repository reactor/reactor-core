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

import org.reactivestreams.Subscriber;
import reactor.util.context.Context;

/**
 * Materialize current {@link Context} from the subscribing flow
 */
final class MonoCurrentContext extends Mono<Context> {

	static final MonoCurrentContext INSTANCE = new MonoCurrentContext();

	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super Context> s, Context ctx) {
		//FIXME should not do this if context lifecycle was subscribe time only
		Context.push(s, ctx);
		ctx = Context.from(s);
		//...

		if (ctx != Context.empty()) {
			s.onSubscribe(Operators.scalarSubscription(s, ctx));
		}
		else {
			Operators.complete(s);
		}
	}
}
