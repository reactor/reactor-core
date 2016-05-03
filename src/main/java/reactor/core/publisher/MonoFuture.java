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
package reactor.core.publisher;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.reactivestreams.Subscriber;
import reactor.core.flow.Fuseable;
import reactor.core.subscriber.DeferredScalarSubscriber;

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class MonoFuture<T> extends Mono<T> implements Fuseable {
	
	final Future<? extends T> future;
	
	final long timeout;

	public MonoFuture(Future<? extends T> future) {
		this.future = future;
		this.timeout = 0L;
	}

	public MonoFuture(Future<? extends T> future, long timeout) {
		this.future = future;
		this.timeout = timeout;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		DeferredScalarSubscriber<T, T> sds = new DeferredScalarSubscriber<>(s);
		
		s.onSubscribe(sds);
		
		T v;
		try {
			if (timeout > 0L) {
				v = future.get(timeout, TimeUnit.MILLISECONDS);
			} else {
				v = future.get();
			}
		} catch (InterruptedException | ExecutionException | TimeoutException ex) {
			s.onError(ex);
			return;
		}
		
		sds.complete(v);
	}
	
}
