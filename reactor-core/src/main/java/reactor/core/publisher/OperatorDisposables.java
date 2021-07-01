/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.util.annotation.Nullable;

/**
 * Utility methods to work with {@link Disposable} atomically.
 *
 * @author Simon Baslé
 * @author David Karnok
 */
final class OperatorDisposables {

	/**
	 * A singleton {@link Disposable} that represents a disposed instance. Should not be
	 * leaked to clients.
	 */
	//NOTE: There is a private similar DISPOSED singleton in DefaultDisposable as well
	static final Disposable DISPOSED = Disposables.disposed();

	/**
	 * Atomically set the field to a {@link Disposable} and dispose the old content.
	 *
	 * @param updater the target field updater
	 * @param holder the target instance holding the field
	 * @param newValue the new Disposable to set
	 * @return true if successful, false if the field contains the {@link #DISPOSED} instance.
	 */
	public static <T> boolean set(AtomicReferenceFieldUpdater<T, Disposable> updater, T holder, @Nullable Disposable newValue) {
		for (;;) {
			Disposable current = updater.get(holder);
			if (current == DISPOSED) {
				if (newValue != null) {
					newValue.dispose();
				}
				return false;
			}
			if (updater.compareAndSet(holder, current, newValue)) {
				if (current != null) {
					current.dispose();
				}
				return true;
			}
		}
	}

	/**
	 * Atomically set the field to the given non-null {@link Disposable} and return true,
	 * or return false if the field is non-null.
	 * If the target field contains the common {@link #DISPOSED} instance, the supplied disposable
	 * is disposed. If the field contains other non-null {@link Disposable}, an {@link IllegalStateException}
	 * is signalled to the {@code errorCallback}.
	 *
	 * @param updater the target field updater
	 * @param holder the target instance holding the field
	 * @param newValue the new Disposable to set, not null
	 * @return true if the operation succeeded, false
	 */
	public static <T> boolean setOnce(AtomicReferenceFieldUpdater<T, Disposable> updater, T holder, Disposable newValue,
			Consumer<RuntimeException> errorCallback) {
		Objects.requireNonNull(newValue, "newValue is null");
		if (!updater.compareAndSet(holder, null, newValue)) {
			newValue.dispose();
			if (updater.get(holder) != DISPOSED) {
				errorCallback.accept(new IllegalStateException("Disposable already pushed"));
			}
			return false;
		}
		return true;
	}

	/**
	 * Atomically replace the {@link Disposable} in the field with the given new Disposable
	 * but do not dispose the old one.
	 *
	 * @param updater the target field updater
	 * @param holder the target instance holding the field
	 * @param newValue the new Disposable to set, null allowed
	 * @return true if the operation succeeded, false if the target field contained
	 * the common {@link #DISPOSED} instance and the given disposable is not null but is disposed.
	 */
	public static <T> boolean replace(AtomicReferenceFieldUpdater<T, Disposable> updater, T holder, @Nullable Disposable newValue) {
		for (;;) {
			Disposable current = updater.get(holder);
			if (current == DISPOSED) {
				if (newValue != null) {
					newValue.dispose();
				}
				return false;
			}
			if (updater.compareAndSet(holder, current, newValue)) {
				return true;
			}
		}
	}

	/**
	 * Atomically dispose the {@link Disposable} in the field if not already disposed.
	 *
	 * @param updater the target field updater
	 * @param holder the target instance holding the field
	 * @return true if the {@link Disposable} held by the field was properly disposed
	 */
	public static <T> boolean dispose(AtomicReferenceFieldUpdater<T, Disposable> updater, T holder) {
		Disposable current = updater.get(holder);
		Disposable d = DISPOSED;
		if (current != d) {
			current = updater.getAndSet(holder, d);
			if (current != d) {
				if (current != null) {
					current.dispose();
				}
				return true;
			}
		}
		return false;
	}

	/**
	 * Verify that current is null and next is not null, otherwise signal a
	 * {@link NullPointerException} to the {@code errorCallback} and return false.
	 *
	 * @param current the current {@link Disposable}, expected to be null
	 * @param next the next {@link Disposable}, expected to be non-null
	 * @return true if the validation succeeded
	 */
	public static boolean validate(@Nullable Disposable current, Disposable next,
			Consumer<RuntimeException> errorCallback) {
		//noinspection ConstantConditions
		if (next == null) {
			errorCallback.accept(new NullPointerException("next is null"));
			return false;
		}
		if (current != null) {
			next.dispose();
			errorCallback.accept(new IllegalStateException("Disposable already pushed"));
			return false;
		}
		return true;
	}

	/**
	 * Atomically try to set the given {@link Disposable} on the field if it is null or
	 * disposes it if the field contains {@link #DISPOSED}.
	 *
	 * @param updater the target field updater
	 * @param holder the target instance holding the field
	 * @param newValue the disposable to set
	 * @return true if successful, false otherwise
	 */
	public static <T> boolean trySet(AtomicReferenceFieldUpdater<T, Disposable> updater, T holder, Disposable newValue) {
		if (!updater.compareAndSet(holder, null, newValue)) {
			if (updater.get(holder) == DISPOSED) {
				newValue.dispose();
			}
			return false;
		}
		return true;
	}

	/**
	 * Check if the given {@link Disposable} is the singleton {@link #DISPOSED}.
	 *
	 * @param d the disposable to check
	 * @return true if d is {@link #DISPOSED}
	 */
	public static boolean isDisposed(Disposable d) {
		return d == DISPOSED;
	}

}