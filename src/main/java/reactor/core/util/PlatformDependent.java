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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import sun.misc.Unsafe;

/**
 *
 * Expose some runtime properties such as Unsafe access or Android environment.
 *
 * Borrowed from Netty project which itself borrows from JCTools and various other projects.
 *
 * Original Reference :
 * <a href='https://github.com/netty/netty/blob/master/common/src/main/java/io/netty/util/internal/PlatformDependent.java'>Netty</a>.
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
	public static final long     DEFAULT_TIMEOUT                 =
			Long.parseLong(System.getProperty("reactor.await.defaultTimeout", "30000"));
	/**
	 * Whether the RingBuffer*Processor can be graphed by wrapping the individual Sequence with the target downstream
	 */
	public static final  boolean TRACEABLE_RING_BUFFER_PROCESSOR =
			Boolean.parseBoolean(System.getProperty("reactor.ringbuffer.trace", "true"));
	/**
	 * Default number of processors available to the runtime on init (min 4)
	 * @see Runtime#availableProcessors()
	 */
	public static final int DEFAULT_POOL_SIZE = Math.max(Runtime.getRuntime()
	                                                            .availableProcessors(), 4);

	private static final boolean HAS_UNSAFE                      = hasUnsafe0();

	@SuppressWarnings("unchecked")
	public static <U, W> AtomicReferenceFieldUpdater<U, W> newAtomicReferenceFieldUpdater(
			Class<U> tclass, String fieldName) {
		if (hasUnsafe()) {
			try {
				return UnsafeSupport.newAtomicReferenceFieldUpdater(tclass, fieldName);
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
		return (T) UnsafeSupport.getUnsafe();
	}

	private static boolean isAndroid() {
		boolean android;
		try {
			Class.forName("android.app.Application", false, UnsafeSupport.getSystemClassLoader());
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
			return UnsafeSupport.hasUnsafe();
		} catch (Throwable t) {
			// Probably failed to initialize PlatformDependent0.
			return false;
		}
	}


	/**
	 * Borrowed from Netty project which itself borrows from JCTools and various other projects.
	 *
	 * @see <a href="https://github.com/netty/netty/blob/master/common/src/main/java/io/netty/util/internal/PlatformDependent.java">Netty javadoc</a>.
	 * operations which requires access to {@code sun.misc.*}.
	 */
	private enum UnsafeSupport {
	;

		private static final Unsafe UNSAFE;


		static {
			ByteBuffer direct = ByteBuffer.allocateDirect(1);
			Field addressField;
			try {
				addressField = Buffer.class.getDeclaredField("address");
				addressField.setAccessible(true);
				if (addressField.getLong(ByteBuffer.allocate(1)) != 0) {
					// A heap buffer must have 0 address.
					addressField = null;
				} else {
					if (addressField.getLong(direct) == 0) {
						// A direct buffer must have non-zero address.
						addressField = null;
					}
				}
			} catch (Throwable t) {
				// Failed to access the address field.
				addressField = null;
			}
			Unsafe unsafe;
			if (addressField != null) {
				try {
					Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
					unsafeField.setAccessible(true);
					unsafe = (Unsafe) unsafeField.get(null);

					// Ensure the unsafe supports all necessary methods to work around the mistake in the latest OpenJDK.
					// https://github.com/netty/netty/issues/1061
					// http://www.mail-archive.com/jdk6-dev@openjdk.java.net/msg00698.html
					try {
						if (unsafe != null) {
							unsafe.getClass().getDeclaredMethod(
									"copyMemory", Object.class, long.class, Object.class, long.class, long.class);
						}
					} catch (NoSuchMethodError | NoSuchMethodException t) {
						throw t;
					}
				} catch (Throwable cause) {
					// Unsafe.copyMemory(Object, long, Object, long, long) unavailable.
					unsafe = null;
				}
			} else {
				// If we cannot access the address of a direct buffer, there's no point of using unsafe.
				// Let's just pretend unsafe is unavailable for overall simplicity.
				unsafe = null;
			}

			UNSAFE = unsafe;
		}

		static boolean hasUnsafe() {
			return UNSAFE != null;
		}

		public static Unsafe getUnsafe(){
			return UNSAFE;
		}


		static <U, W> AtomicReferenceFieldUpdater<U, W> newAtomicReferenceFieldUpdater(
				Class<U> tclass, String fieldName) throws Exception {
			return new UnsafeAtomicReferenceFieldUpdater<U, W>(tclass, fieldName);
		}

		static ClassLoader getSystemClassLoader() {
			if (System.getSecurityManager() == null) {
				return ClassLoader.getSystemClassLoader();
			} else {
				return AccessController.doPrivileged((PrivilegedAction<ClassLoader>) ClassLoader::getSystemClassLoader);
			}
		}

		UnsafeSupport() {
		}

		private static final class UnsafeAtomicReferenceFieldUpdater<U, M> extends AtomicReferenceFieldUpdater<U, M> {
			private final long offset;

			UnsafeAtomicReferenceFieldUpdater(Class<U> tClass, String fieldName) throws NoSuchFieldException {
				Field field = tClass.getDeclaredField(fieldName);
				if (!Modifier.isVolatile(field.getModifiers())) {
					throw new IllegalArgumentException("Must be volatile");
				}
				offset = UNSAFE.objectFieldOffset(field);
			}

			@Override
			public boolean compareAndSet(U obj, M expect, M update) {
				return UNSAFE.compareAndSwapObject(obj, offset, expect, update);
			}

			@Override
			public boolean weakCompareAndSet(U obj, M expect, M update) {
				return UNSAFE.compareAndSwapObject(obj, offset, expect, update);
			}

			@Override
			public void set(U obj, M newValue) {
				UNSAFE.putObjectVolatile(obj, offset, newValue);
			}

			@Override
			public void lazySet(U obj, M newValue) {
				UNSAFE.putOrderedObject(obj, offset, newValue);
			}

			@SuppressWarnings("unchecked")
			@Override
			public M get(U obj) {
				return (M) UNSAFE.getObjectVolatile(obj, offset);
			}
		}
	}
}
