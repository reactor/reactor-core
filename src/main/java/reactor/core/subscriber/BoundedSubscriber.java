/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.subscriber;

import java.util.function.Consumer;

import org.reactivestreams.Subscription;

/**
 * @author Stephane Maldini
 */
final class BoundedSubscriber<T> extends LambdaSubscriber<T>  {

	final int capacity;
	final int limit;

	int outstanding;

	BoundedSubscriber(int capacity,
			Consumer<? super T> consumer,
			Consumer<? super Throwable> errorConsumer,
			Runnable completeConsumer) {
		this(capacity, capacity / 4, consumer, errorConsumer, completeConsumer);
	}

	BoundedSubscriber(int capacity,
			int limit,
			Consumer<? super T> consumer,
			Consumer<? super Throwable> errorConsumer,
			Runnable completeConsumer) {
		super(consumer, errorConsumer, completeConsumer);
		this.limit = limit;
		this.capacity = capacity;
		this.outstanding = capacity;
	}

	@Override
	protected void doNext(T x) {
		super.doNext(x);
		int r = outstanding - 1;
		if(r > limit){
			outstanding = r;
			return;
		}

		int k = capacity - r;
		outstanding = capacity;
		requestMore(k);
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		subscription.request(capacity);
	}

	@Override
	public long getCapacity() {
		return capacity;
	}

	@Override
	public long limit() {
		return limit;
	}

	@Override
	public long expectedFromUpstream() {
		return outstanding;
	}
}
