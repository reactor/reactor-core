/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

/**
 * A common interface of operator in Reactor which adds contracts around subscription
 * optimizations:
 * <ul>
 *     <li>looping instead of recursive subscribes, via {@link #subscribeOrReturn(CoreSubscriber)} and
 *     {@link #nextOptimizableSource()}</li>
 * </ul>
 *
 * @param <IN> the {@link CoreSubscriber} data type
 * @since 3.3.0
 */
interface OptimizableOperator<IN, OUT> extends CorePublisher<IN> {

	/**
	 * Allow delegation of the subscription by returning a {@link CoreSubscriber}, or force
	 * subscription encapsulation by returning null. This can be used in conjunction with {@link #nextOptimizableSource()}
	 * to perform subscription in a loop instead of by recursion.
	 *
	 * @return next {@link CoreSubscriber} or "null" if the subscription was already done inside the method
	 */
	@Nullable
	CoreSubscriber<? super OUT> subscribeOrReturn(CoreSubscriber<? super IN> actual) throws Throwable;

	/**
	 * @return {@link CorePublisher} to call {@link CorePublisher#subscribe(CoreSubscriber)} on
	 * if {@link #nextOptimizableSource()} have returned null result
	 */
	CorePublisher<? extends OUT> source();

	/**
	 * Allow delegation of the subscription by returning the next {@link OptimizableOperator} UP in the
	 * chain, to be subscribed to next. This method MUST return a non-null value if the {@link #subscribeOrReturn(CoreSubscriber)}
	 * method returned a non null value. In that case, this next operator can be used in conjunction with {@link #subscribeOrReturn(CoreSubscriber)}
	 * to perform subscription in a loop instead of by recursion.
	 *
	 * @return next {@link OptimizableOperator} if {@link #subscribeOrReturn(CoreSubscriber)} have returned non-null result
	 */
	@Nullable
	OptimizableOperator<?, ? extends OUT> nextOptimizableSource();
}
