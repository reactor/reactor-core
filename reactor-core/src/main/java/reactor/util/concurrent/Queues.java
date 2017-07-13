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

package reactor.util.concurrent;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;

/**
 * Utility methods around {@link Queue Queues} and pre-defined {@link QueueSupplier suppliers}.
 */
public class Queues {

	public static final int CAPACITY_UNSURE = Integer.MIN_VALUE;

	/**
	 * Return the capacity of a given {@link Queue} in a best effort fashion. Queues that
	 * are known to be unbounded will return {@code Integer.MAX_VALUE} and queues that
	 * have a known bounded capacity will return that capacity. For other {@link Queue}
	 * implementations not recognized by this method or not providing this kind of
	 * information, {@link #CAPACITY_UNSURE} ({@code Integer.MIN_VALUE}) is returned.
	 *
	 * @param q the {@link Queue} to try to get a capacity for
	 * @return the capacity of the queue, if discoverable with confidence, or {@link #CAPACITY_UNSURE} negative constant.
	 */
	public static final int capacity(Queue q) {
		if (q instanceof BlockingQueue) {
			return ((BlockingQueue) q).remainingCapacity();
		}
		else if (q instanceof SpscLinkedArrayQueue) {
			return Integer.MAX_VALUE;
		}
		else if (q instanceof SpscArrayQueue) {
			return ((SpscArrayQueue) q).length();
		}
		else {
			return CAPACITY_UNSURE;
		}
	}

}
