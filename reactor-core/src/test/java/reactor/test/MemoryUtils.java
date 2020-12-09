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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
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

	/**
	 * A utility class to check that some {@link Tracked} objects (emulating off heap objects)
	 * are explicitly released.
	 */
	public static final class OffHeapDetector {

		private final Queue<Tracked> tracker;


		public OffHeapDetector() {
			//note: AssertJ representation of Tracked is installed in ReactorTestExecutionListener
			tracker = new ConcurrentLinkedQueue<>();
		}

		/**
		 * Create a {@link Tracked} object with the given {@link String} identifier.
		 *
		 * @param identifier the identifier for the tracked object
		 * @return the new tracked object to be manually released
		 */
		public final Tracked track(String identifier) {
			Tracked tracked = new Tracked(identifier);
			tracker.add(tracked);
			return tracked;
		}

		/**
		 * Create a {@link Tracked} object from an int id, for convenience.
		 *
		 * @param identifier the identifier for the tracked object
		 * @return the new tracked object to be manually released
		 */
		public final Tracked track(int identifier) {
			return track("" + identifier);
		}

		/**
		 * Return the total number of tracked objects so far.
		 *
		 * @return the number of tracked objects
		 */
		public final long trackedTotal() {
			return tracker.size();
		}

		/**
		 * Assert that all currently tracked objects have been {@link Tracked#release() released}.
		 * <p>
		 * Note that the {@link Tracked} object has a custom AssertJ representation (otherwise it
		 * would be represented as an {@link AtomicBoolean}). It is automatically registered in
		 * {@link AssertionsUtils} whenever an {@link OffHeapDetector} is instantiated.
		 */
		public void assertNoLeaks() {
			Assertions.assertThat(tracker).allMatch(Tracked::isReleased, "isReleased");
		}

		/**
		 * Reset this tracker, clearing the queue of tracked objects (which can be useful eg. when
		 * looping multiple times in order to test race conditions).
		 */
		public void reset() {
			tracker.clear();
		}
	}

	/**
	 * An object emulating off-heap objects that need to be manually {@link #release() released}.
	 * This is simply an {@link AtomicBoolean} that gets switched when released.
	 * <p>
	 * Use {@link #safeRelease(Object)} in generic object hooks to release a hook-provided object
	 * if it is an instance of {@link Tracked}.
	 * <p>
	 * Note that {@link AssertionsUtils#installAssertJTestRepresentation()}'s AssertJ {@link org.assertj.core.presentation.Representation}
	 * recognizes this class.
	 */
	public static final class Tracked extends AtomicBoolean {

		/**
		 * A pre-released {@link Tracked} instance for convenience in some tests.
		 */
		public static final Tracked RELEASED = new Tracked("RELEASED", true);

		/**
		 * Check if an arbitrary object is a {@link Tracked}, and if so release it.
		 *
		 * @param t the arbitrary object
		 */
	    public static void safeRelease(Object t) {
	        if (t instanceof Tracked) {
	            ((Tracked) t).release();
	        }
	    }

		/**
		 * An identifier for the tracked object, which can help debugging when tests fail.
		 */
		public final String identifier;

	    Tracked(String identifier) {
	        this.identifier = identifier;
	    }

	    Tracked(String identifier, boolean preReleased) {
	    	this.identifier = identifier;
	    	set(preReleased);
	    }

		/**
		 * Release this {@link Tracked} object.
		 */
		public void release() {
	        set(true);
	    }

		/**
		 * Check if this {@link Tracked} object has been released.
		 *
		 * @return true if released, false otherwise
		 */
		public boolean isReleased() {
	        return get();
	    }

	    @Override
	    public boolean equals(Object o) {
	        if (this == o) return true;
	        if (o == null || getClass() != o.getClass()) return false;

	        Tracked tracked = (Tracked) o;

	        return identifier.equals(tracked.identifier);
	    }

	    @Override
	    public int hashCode() {
	        return identifier.hashCode();
	    }

	    //NOTE: AssertJ has a special representation of AtomicBooleans, so we override it in AssertionsUtils
	    @Override
	    public String toString() {
	        return "Tracked{" +
	                " id=" + identifier +
	                " released=" + get() +
	                " }";
	    }
	}
}
