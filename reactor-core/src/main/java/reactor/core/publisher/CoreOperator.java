/*
 * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
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

import org.reactivestreams.Publisher;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

/**
 * A common interface of any operator in Reactor.
 *
 *
 * @param <IN> the {@link CoreSubscriber} data type
 *
 * @since 3.3.0
 */
interface CoreOperator<IN, OUT> {

	/**
	 *
	 * @return next {@link CoreSubscriber} or "null" if the subscription was already done inside the method
	 */
	@Nullable
	CoreSubscriber<? super OUT> subscribeOrReturn(CoreSubscriber<? super IN> actual);

	/**
	 *
	 * @return {@link Publisher} to call {@link CorePublisher#subscribe(CoreSubscriber)} on
	 * if {@link #subscribeOrReturn(CoreSubscriber)} have returned non-null result
	 */
	Publisher<? extends OUT> source();
}
