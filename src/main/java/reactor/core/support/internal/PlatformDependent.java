/*
 * Copyright (c) 2011-2016 Pivotal Software Inc., Inc. All Rights Reserved.
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
package reactor.core.support.internal;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Borrowed from Netty project which itself borrows from JCTools and various other projects.
 *
 * Expose some runtime properties such as Unsafe access or Android environment.
 *
 * @see <a href="https://github.com/netty/netty/blob/master/common/src/main/java/io/netty/util/internal/PlatformDependent.java">Netty javadoc</a>.
 */
public class PlatformDependent {

	private static final boolean HAS_UNSAFE = hasUnsafe0();

	private PlatformDependent() {
	}

	@SuppressWarnings("unchecked")
	public static <U, W> AtomicReferenceFieldUpdater<U, W> newAtomicReferenceFieldUpdater(
			Class<U> tclass, String fieldName) {
		if (hasUnsafe()) {
			try {
				return PlatformDependent0.newAtomicReferenceFieldUpdater(tclass, fieldName);
			} catch (Throwable ignore) {
				// ignore
			}
		}
		return AtomicReferenceFieldUpdater.newUpdater(tclass, (Class<W>)Object.class, fieldName);
	}

	/**
	 * Return {@code true} if {@code sun.misc.Unsafe} was found on the classpath and can be used for acclerated
	 * direct memory access.
	 */
	public static boolean hasUnsafe() {
		return HAS_UNSAFE;
	}

	private static boolean isAndroid() {
		boolean android;
		try {
			Class.forName("android.app.Application", false, PlatformDependent0.getSystemClassLoader());
			android = true;
		} catch (Exception e) {
			// Failed to load the class uniquely available in Android.
			android = false;
		}

		return android;
	}

	private static boolean hasUnsafe0() {

		if (isAndroid()) {
			return false;
		}

		try {
			return PlatformDependent0.hasUnsafe();
		} catch (Throwable t) {
			// Probably failed to initialize PlatformDependent0.
			return false;
		}
	}
}
