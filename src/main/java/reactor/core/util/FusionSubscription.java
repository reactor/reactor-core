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
package reactor.core.util;

import java.util.Queue;

import org.reactivestreams.Subscription;

/**
 * Contract queue-fusion based optimizations for supporting subscriptions.
 *
 * <ul>
 *  <li>
 *  Synchronous sources which have fixed size and can
 *  emit its items in a pull fashion, thus avoiding the request-accounting
 *  overhead in many cases.
 *  </li>
 *  <li>
 *  Asynchronous sources which can act as a queue and subscription at
 *  the same time, saving on allocating another queue most of the time.
 * </li>
 * </ul>
 *
 * <p>
 *
 * @param <T> the value type emitted
 */
public interface FusionSubscription<T> extends Queue<T>, Subscription {

	/**
	 * Consumers of an Asynchronous FusionSubscription have to signal it to switch to a fused-mode
	 * so it no longer run its own drain loop but directly signals onNext(null) to
	 * indicate there is an item available in this queue-view. other interaction with the Subscription.
	 * Because it can't be immediately fully consumed, the method will return false.
	 * <p>
	 * Consumers of an Synchronous FusionSubscription will usually consider this method no-op and
	 * return true to signal immediate availability.
	 * <p>
	 * The method has to be called while the parent is in onSubscribe and before any
	 *
	 * @return FALSE if asynchronous or TRUE if immediately ready
	 */
	boolean enableOperatorFusion();


}
