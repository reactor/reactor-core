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
package reactor.core.subscription;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * A singleton enumeration that represents a no-op Subscription instance that can be freely given out to clients.
 */
public enum EmptySubscription implements Subscription {
	INSTANCE;

	@Override
	public void request(long n) {
		// deliberately no op
	}

	@Override
	public void cancel() {
		// deliberately no op
	}

	/**
	 * Calls onSubscribe on the target Subscriber with the empty instance followed by a call to onError with the
	 * supplied error.
	 *
	 * @param s
	 * @param e
	 */
	public static void error(Subscriber<?> s, Throwable e) {
		s.onSubscribe(INSTANCE);
		s.onError(e);
	}

	/**
	 * Calls onSubscribe on the target Subscriber with the empty instance followed by a call to onComplete.
	 *
	 * @param s
	 */
	public static void complete(Subscriber<?> s) {
		s.onSubscribe(INSTANCE);
		s.onComplete();
	}
}
