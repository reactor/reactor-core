/*
 * Copyright (c) 2015-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core;

import java.util.Collection;
import java.util.function.Supplier;

import reactor.util.annotation.Nullable;

/**
 * Indicates that a task or resource can be cancelled/disposed.
 * <p>Call to the dispose method is/should be idempotent.
 */
@FunctionalInterface
public interface Disposable {

	/**
	 * Cancel or dispose the underlying task or resource.
	 * <p>
	 * Implementations are required to make this method idempotent.
	 */
	void dispose();

	/**
	 * Optionally return {@literal true} when the resource or task is disposed.
	 * <p>
	 * Implementations are not required to track disposition and as such may never
	 * return {@literal true} even when disposed. However, they MUST only return true
	 * when there's a guarantee the resource or task is disposed.
	 *
	 * @return {@literal true} when there's a guarantee the resource or task is disposed.
	 */
	default boolean isDisposed() {
		return false;
	}

	/**
	 * A {@link Disposable} container that allows updating/replacing its inner Disposable
	 * atomically and with respect of disposing the container itself.
	 *
	 * @author Simon Baslé
	 */
	interface Swap extends Disposable, Supplier<Disposable> {

		/**
		 * Atomically set the next {@link Disposable} on this container and dispose the previous
		 * one (if any). If the container has been disposed, fall back to disposing {@code next}.
		 *
		 * @param next the {@link Disposable} to set, may be null
		 * @return true if the operation succeeded, false if the container has been disposed
		 * @see #replace(Disposable)
		 */
		boolean update(@Nullable Disposable next);

		/**
		 * Atomically set the next {@link Disposable} on this container but don't dispose the previous
		 * one (if any). If the container has been disposed, fall back to disposing {@code next}.
		 *
		 * @param next the {@link Disposable} to set, may be null
		 * @return true if the operation succeeded, false if the container has been disposed
		 * @see #update(Disposable)
		 */
		boolean replace(@Nullable Disposable next);
	}

	/**
	 * A container of {@link Disposable} that is itself {@link Disposable}. Accumulate
	 * disposables and dispose them all in one go by using {@link #dispose()}. Using
	 * the {@link #add(Disposable)} methods give ownership to the container, which is now
	 * responsible for disposing them. You can however retake ownership of individual
	 * elements by keeping a reference and using {@link #remove(Disposable)}, which puts
	 * the responsibility of disposing said elements back in your hands. Note that once
	 * disposed, the container cannot be reused and you will need a new {@link Composite}.
	 *
	 * @author Simon Baslé
	 */
	interface Composite extends Disposable {

		/**
		 * Add a {@link Disposable} to this container, if it is not {@link #isDisposed() disposed}.
		 * Otherwise d is disposed immediately.
		 *
		 * @param d the {@link Disposable} to add.
		 * @return true if the disposable could be added, false otherwise.
		 */
		boolean add(Disposable d);

		/**
		 * Adds the given collection of Disposables to the container or disposes them
		 * all if the container has been disposed.
		 *
		 * @implNote The default implementation is not atomic, meaning that if the container is
		 * disposed while the content of the collection is added, first elements might be
		 * effectively added. Stronger consistency is enforced by composites created via
		 * {@link Disposables#composite()} variants.
		 * @param ds the collection of Disposables
		 * @return true if the operation was successful, false if the container has been disposed
		 */
		default boolean addAll(Collection<? extends Disposable> ds) {
			boolean abort = isDisposed();
			for (Disposable d : ds) {
				if (abort) {
					//multi-add aborted,
					d.dispose();
				}
				else {
					//update the abort flag by attempting to add the disposable
					//if not added, it has already been disposed. we abort and will
					//dispose all subsequent Disposable
					abort = !add(d);
				}
			}
			return !abort;
		}

		/**
		 * Atomically mark the container as {@link #isDisposed() disposed}, clear it and then
		 * dispose all the previously contained Disposables. From there on the container cannot
		 * be reused, as {@link #add(Disposable)} and {@link #addAll(Collection)} methods
		 * will immediately return {@literal false}.
		 */
		@Override
		void dispose();

		/**
		 * Indicates if the container has already been disposed.
		 * <p>Note that if that is the case, attempts to add new disposable to it via
		 * {@link #add(Disposable)} and {@link #addAll(Collection)} will be rejected.
		 *
		 * @return true if the container has been disposed, false otherwise.
		 */
		@Override
		boolean isDisposed();

		/**
		 * Delete the {@link Disposable} from this container, without disposing it.
		 * <p>
		 * It becomes the responsibility of the caller to dispose the value themselves,
		 * which they can do by a simple call to {@link Disposable#dispose()} on said
		 * value (probably guarded by a check that this method returned true, meaning the
		 * disposable was actually in the container).
		 *
		 * @param d the {@link Disposable} to remove.
		 * @return true if the disposable was successfully deleted, false otherwise.
		 */
		boolean remove(Disposable d);

		/**
		 * Returns the number of currently held Disposables.
		 * @return the number of currently held Disposables
		 */
		int size();
	}
}
