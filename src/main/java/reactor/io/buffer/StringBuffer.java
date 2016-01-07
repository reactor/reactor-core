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
package reactor.io.buffer;

import java.nio.ByteBuffer;

/**
 * @author Stephane Maldini
 */
public final class StringBuffer extends Buffer {

	public StringBuffer() {
		super();
	}

	public StringBuffer(ByteBuffer buffer) {
		super(buffer);
	}

	public StringBuffer(int atLeast, boolean fixed) {
		super(atLeast, fixed);
	}

	/**
	 * Convenience method to create a new, fixed-length {@literal Buffer} and putting the given byte array into the
	 * buffer.
	 *
	 * @param bytes The bytes to create a buffer from.
	 * @return The new {@literal Buffer}.
	 */
	@SuppressWarnings("resource")
	public static Buffer wrap(byte[] bytes) {
		return new StringBuffer(bytes.length, true)
				.append(bytes)
				.flip();
	}
	/**
	 * Convenience method to create a new, fixed-length {@literal Buffer} and putting the given byte array into the
	 * buffer.
	 *
	 * @param str The String to create a buffer from.
	 * @return The new {@literal Buffer}.
	 */
	@SuppressWarnings("resource")
	public static Buffer wrap(String str) {
		return wrap(str.getBytes());
	}

	@Override
	public Buffer newBuffer() {
		return new StringBuffer();
	}

	@Override
	public String toString() {
		return asString();
	}
}
