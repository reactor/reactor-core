/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.test;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;

/**
 * Test utility around memory, references, leaks and retained object detection.
 *
 * @author Simon Baslé
 */
public class MemoryUtils {

	/**
	 * A utility class to check that some tracked object are finalized, by way of tracking
	 * them through a {@link PhantomReference}.
	 */
	public static final class RetainedDetector {

		private final ReferenceQueue<Object>         referenceQueue    = new ReferenceQueue<>();
		private final List<PhantomReference<Object>> phantomReferences = new LinkedList<>();
		private long finalizedSoFar = 0L;
		private AtomicInteger trackedTotal = new AtomicInteger(0);

		/**
		 * Track the object in this {@link RetainedDetector}'s {@link ReferenceQueue}
		 *
		 * @param object the object to track
		 * @param <T> the type of the object
		 * @return the tracked object for further use
		 */
		public final <T> T tracked(T object) {
			phantomReferences.add(new PhantomReference<>(object, referenceQueue));
			trackedTotal.incrementAndGet();
			return object;
		}

		/**
		 * Returns the number of tracked objects that have been finalized.
		 * @return the number of tracked object that have been finalized.
		 */
		public final synchronized long finalizedCount() {
			synchronized (this) {
				while(referenceQueue.poll() != null) {
					finalizedSoFar++;
				}
			}
			return finalizedSoFar;
		}

		/**
		 * @return the total number of objects that have been added to this {@link RetainedDetector}
		 */
		public final long trackedTotal() {
			return trackedTotal.get();
		}

		/**
		 * Assert that all tracked elements have been finalized.
		 * @throws AssertionError if some tracked elements have not been finalized
		 */
		public final void assertAllFinalized() {
			Assertions.assertThat(this.finalizedCount()).as("all tracked finalized").isEqualTo(trackedTotal.get());
		}
	}

}
