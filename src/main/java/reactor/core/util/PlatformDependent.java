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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 *
 * Expose some runtime properties such as Unsafe access or Android environment.
 *
 * Borrowed from Netty project which itself borrows from JCTools and various other projects.
 * @see <a href="https://github.com/netty/netty/blob/master/common/src/main/java/io/netty/util/internal/PlatformDependent.java">Netty javadoc</a>.
 */
public enum PlatformDependent {
	;

	/**
	 *
	 */
	public static final  boolean TRACE_CANCEL                    =
			Boolean.parseBoolean(System.getProperty("reactor.trace.cancel", "false"));
	/**
	 *
	 */
	public static final  boolean TRACE_TIMEROVERLOW              =
			Boolean.parseBoolean(System.getProperty("reactor.trace.timeroverflow", "false"));
	/**
	 *
	 */
	public static final  boolean TRACE_NOCAPACITY                =
			Boolean.parseBoolean(System.getProperty("reactor.trace.nocapacity", "false"));
	/**
	 * An allocation friendly default of available slots in a given container, e.g. slow publishers and or fast/few
	 * subscribers
	 */
	public static final  int     XS_BUFFER_SIZE                  = 32;
	/**
	 * A small default of available slots in a given container, compromise between intensive pipelines, small
	 * subscribers numbers and memory use.
	 */
	public static final  int     SMALL_BUFFER_SIZE               = 256;
	/**
	 * A larger default of available slots in a given container, e.g. mutualized processors, intensive pipelines or
	 * larger subscribers number
	 */
	public static final  int     MEDIUM_BUFFER_SIZE              = 8192;
	/**
	 * The size, in bytes, of a small buffer. Can be configured using the {@code reactor.io.defaultBufferSize} system
	 * property. Default to 16384 bytes.
	 */
	public static final  int     SMALL_IO_BUFFER_SIZE            =
			Integer.parseInt(System.getProperty("reactor.io.defaultBufferSize", "" + 1024 * 16));
	/**
	 * The maximum allowed buffer size in bytes. Can be configured using the {@code reactor.io.maxBufferSize} system
	 * property. Defaults to 16384000 bytes.
	 */
	public static final  int     MAX_IO_BUFFER_SIZE              =
			Integer.parseInt(System.getProperty("reactor.io.maxBufferSize", "" + 1024 * 1000 * 16));
	/**
	 *
	 */
	public static final  long    DEFAULT_TIMEOUT                 =
			Long.parseLong(System.getProperty("reactor.await.defaultTimeout", "30000"));
	/**
	 * Whether the RingBuffer*Processor can be graphed by wrapping the individual Sequence with the target downstream
	 */
	public static final  boolean TRACEABLE_RING_BUFFER_PROCESSOR =
			Boolean.parseBoolean(System.getProperty("reactor.ringbuffer.trace", "true"));
	private static final boolean HAS_UNSAFE                      = hasUnsafe0();

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

	/**
	 * Return {@code true} if {@code sun.misc.Unsafe} was found on the classpath and can be used for acclerated
	 * direct memory access.
	 *
	 * @param <T> the Unsafe type
	 */
	@SuppressWarnings("unchecked")
	public static <T> T getUnsafe() {
		return (T)PlatformDependent0.getUnsafe();
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
