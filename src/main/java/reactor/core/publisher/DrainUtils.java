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

import java.util.Queue;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BooleanSupplier;

import org.reactivestreams.Subscriber;
import reactor.core.util.BackpressureUtils;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
enum DrainUtils {
	;


	static final long COMPLETED_MASK = 0x8000_0000_0000_0000L;
	static final long REQUESTED_MASK = 0x7FFF_FFFF_FFFF_FFFFL;

	/**
	 * Perform a potential post-completion request accounting.
	 *
	 * @param n
	 * @param actual
	 * @param queue
	 * @param field
	 * @param isCancelled
	 * @return true if the state indicates a completion state.
	 */
	public static <T, F> boolean postCompleteRequest(long n,
			Subscriber<? super T> actual,
			Queue<T> queue,
			AtomicLongFieldUpdater<F> field,
			F instance,
			BooleanSupplier isCancelled) {

		for (; ; ) {
			long r = field.get(instance);

			// extract the current request amount
			long r0 = r & REQUESTED_MASK;

			// preserve COMPLETED_MASK and calculate new requested amount
			long u = (r & COMPLETED_MASK) | BackpressureUtils.addCap(r0, n);

			if (field.compareAndSet(instance, r, u)) {
				// (complete, 0) -> (complete, n) transition then replay
				if (r == COMPLETED_MASK) {

					postCompleteDrain(n | COMPLETED_MASK, actual, queue, field, instance, isCancelled);

					return true;
				}
				// (active, r) -> (active, r + n) transition then continue with requesting from upstream
				return false;
			}
		}

	}

	/**
	 * Drains the queue either in a pre- or post-complete state.
	 *
	 * @param n
	 * @param actual
	 * @param queue
	 * @param field
	 * @param isCancelled
	 * @return true if the queue was completely drained or the drain process was cancelled
	 */
	static <T, F> boolean postCompleteDrain(long n,
			Subscriber<? super T> actual,
			Queue<T> queue,
			AtomicLongFieldUpdater<F> field,
			F instance,
			BooleanSupplier isCancelled) {

// TODO enable fast-path
//        if (n == -1 || n == Long.MAX_VALUE) {
//            for (;;) {
//                if (isCancelled.getAsBoolean()) {
//                    break;
//                }
//
//                T v = queue.poll();
//
//                if (v == null) {
//                    actual.onComplete();
//                    break;
//                }
//
//                actual.onNext(v);
//            }
//
//            return true;
//        }

		long e = n & COMPLETED_MASK;

		for (; ; ) {

			while (e != n) {
				if (isCancelled.getAsBoolean()) {
					return true;
				}

				T t = queue.poll();

				if (t == null) {
					actual.onComplete();
					return true;
				}

				actual.onNext(t);
				e++;
			}

			if (isCancelled.getAsBoolean()) {
				return true;
			}

			if (queue.isEmpty()) {
				actual.onComplete();
				return true;
			}

			n = field.get(instance);

			if (n == e) {

				n = field.addAndGet(instance, -(e & REQUESTED_MASK));

				if ((n & REQUESTED_MASK) == 0L) {
					return false;
				}

				e = n & COMPLETED_MASK;
			}
		}

	}

	/**
	 * Tries draining the queue if the source just completed.
	 *
	 * @param actual
	 * @param queue
	 * @param field
	 * @param isCancelled
	 */
	public static <T, F> void postComplete(Subscriber<? super T> actual,
			Queue<T> queue,
			AtomicLongFieldUpdater<F> field,
			F instance,
			BooleanSupplier isCancelled) {

		if (queue.isEmpty()) {
			actual.onComplete();
			return;
		}

		if (postCompleteDrain(field.get(instance), actual, queue, field, instance, isCancelled)) {
			return;
		}

		for (; ; ) {
			long r = field.get(instance);

			if ((r & COMPLETED_MASK) != 0L) {
				return;
			}

			long u = r | COMPLETED_MASK;
			// (active, r) -> (complete, r) transition
			if (field.compareAndSet(instance, r, u)) {
				// if the requested amount was non-zero, drain the queue
				if (r != 0L) {
					postCompleteDrain(u, actual, queue, field, instance, isCancelled);
				}

				return;
			}
		}
	}

    /**
     * Perform a potential post-completion request accounting.
     *
     * @param n
     * @param actual
     * @param queue
     * @param field
     * @param isCancelled
     * @param error
     * @return true if the state indicates a completion state.
     */
    public static <T, F> boolean postCompleteRequestDelayError(long n,
            Subscriber<? super T> actual,
            Queue<T> queue,
            AtomicLongFieldUpdater<F> field,
            F instance,
            BooleanSupplier isCancelled, Throwable error) {

        for (; ; ) {
            long r = field.get(instance);

            // extract the current request amount
            long r0 = r & REQUESTED_MASK;

            // preserve COMPLETED_MASK and calculate new requested amount
            long u = (r & COMPLETED_MASK) | BackpressureUtils.addCap(r0, n);

            if (field.compareAndSet(instance, r, u)) {
                // (complete, 0) -> (complete, n) transition then replay
                if (r == COMPLETED_MASK) {

                    postCompleteDrainDelayError(n | COMPLETED_MASK, actual, queue, field, instance, isCancelled, error);

                    return true;
                }
                // (active, r) -> (active, r + n) transition then continue with requesting from upstream
                return false;
            }
        }

    }

    /**
     * Drains the queue either in a pre- or post-complete state.
     *
     * @param n
     * @param actual
     * @param queue
     * @param field
     * @param isCancelled
     * @param error
     * @return true if the queue was completely drained or the drain process was cancelled
     */
    static <T, F> boolean postCompleteDrainDelayError(long n,
            Subscriber<? super T> actual,
            Queue<T> queue,
            AtomicLongFieldUpdater<F> field,
            F instance,
            BooleanSupplier isCancelled, Throwable error) {

        long e = n & COMPLETED_MASK;

        for (; ; ) {

            while (e != n) {
                if (isCancelled.getAsBoolean()) {
                    return true;
                }

                T t = queue.poll();

                if (t == null) {
                    if (error == null) {
                        actual.onComplete();
                    } else {
                        actual.onError(error);
                    }
                    return true;
                }

                actual.onNext(t);
                e++;
            }

            if (isCancelled.getAsBoolean()) {
                return true;
            }

            if (queue.isEmpty()) {
                if (error == null) {
                    actual.onComplete();
                } else {
                    actual.onError(error);
                }
                return true;
            }

            n = field.get(instance);

            if (n == e) {

                n = field.addAndGet(instance, -(e & REQUESTED_MASK));

                if ((n & REQUESTED_MASK) == 0L) {
                    return false;
                }

                e = n & COMPLETED_MASK;
            }
        }

    }

    /**
     * Tries draining the queue if the source just completed.
     *
     * @param actual
     * @param queue
     * @param field
     * @param isCancelled
     * @param error
     */
    public static <T, F> void postCompleteDelayError(Subscriber<? super T> actual,
            Queue<T> queue,
            AtomicLongFieldUpdater<F> field,
            F instance,
            BooleanSupplier isCancelled, Throwable error) {

        if (queue.isEmpty()) {
            if (error == null) {
                actual.onComplete();
            } else {
                actual.onError(error);
            }
            return;
        }

        if (postCompleteDrainDelayError(field.get(instance), actual, queue, field, instance, isCancelled, error)) {
            return;
        }

        for (; ; ) {
            long r = field.get(instance);

            if ((r & COMPLETED_MASK) != 0L) {
                return;
            }

            long u = r | COMPLETED_MASK;
            // (active, r) -> (complete, r) transition
            if (field.compareAndSet(instance, r, u)) {
                // if the requested amount was non-zero, drain the queue
                if (r != 0L) {
                    postCompleteDrainDelayError(u, actual, queue, field, instance, isCancelled, error);
                }

                return;
            }
        }
    }

}
