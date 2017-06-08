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
import org.reactivestreams.Subscription;
import reactor.util.context.ContextRelay;
import reactor.core.Scannable;
import javax.annotation.Nullable;
import reactor.util.context.Context;

/**
 *
 * {@link InnerProducer} is a {@link reactor.core.Scannable} {@link Subscription} that produces
 * data to an {@link #actual()} {@link Subscriber}
 *
 * @param <O> output operator produced type
 *
 * @author Stephane Maldini
 */
interface InnerProducer<O>
		extends ContextRelay, Scannable, Subscription {

	Subscriber<? super O> actual();

	@Override
	default Context currentContext() {
		return ContextRelay.getOrEmpty(actual());
	}

	@Override
	default void onContext(Context context) {
		ContextRelay.set(actual(), context);
	}

	@Override
	@Nullable
	default Object scanUnsafe(Attr key){
		if (key == ScannableAttr.ACTUAL) {
			return actual();
		}
		return null;
	}

}
