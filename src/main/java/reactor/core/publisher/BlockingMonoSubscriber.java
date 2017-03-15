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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Receiver;
import reactor.core.Trackable;

/**
 * Blocks assuming the upstream is a Mono, until it signals its value or completes.
 * Compared to {@link BlockingFirstSubscriber}, this variant doesn't cancel the upstream.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 */
final class BlockingMonoSubscriber<T> extends BlockingSingleSubscriber<T> {

	@Override
	public void onNext(T t) {
		if (value == null) {
			value = t;
			dispose();
			countDown();
		}
	}

	@Override
	public void onError(Throwable t) {
		if (value == null) {
			error = t;
		}
		countDown();
	}

	@Override
	protected void upstreamCancel(Subscription s) {
		//NO-OP
	}
}
