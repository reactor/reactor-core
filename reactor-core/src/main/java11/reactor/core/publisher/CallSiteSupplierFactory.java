/*
 * Copyright (c) 2023-2024 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static reactor.core.publisher.Traces.full;
import static reactor.core.publisher.Traces.isUserCode;
import static reactor.core.publisher.Traces.shouldSanitize;

/**
 * Utility class for the call-site extracting on Java 9+.
 */
final class CallSiteSupplierFactory implements Supplier<Supplier<String>>, Function<Stream<StackWalker.StackFrame>, StackWalker.StackFrame[]> {

	static {
		// Trigger eager StackWalker class loading.
		StackWalker.getInstance();
	}

	@Override
	public StackWalker.StackFrame[] apply(Stream<StackWalker.StackFrame> s) {
		StackWalker.StackFrame[] result =
				new StackWalker.StackFrame[10];
		Iterator<StackWalker.StackFrame> iterator = s.iterator();
		iterator.next(); // .get

		int i = 0;
		while (iterator.hasNext()) {
			StackWalker.StackFrame frame = iterator.next();

			if (i >= result.length) {
				return new StackWalker.StackFrame[0];
			}

			result[i++] = frame;

			if (isUserCode(frame.getClassName())) {
				break;
			}
		}
		StackWalker.StackFrame[] copy =
				new StackWalker.StackFrame[i];
		System.arraycopy(result, 0, copy, 0, i);
		return copy;
	}

	/**
	 * Transform the current stack trace into a {@link String} representation,
	 * each element being prepended with a tabulation and appended with a
	 * newline.
	 *
	 * @return the string version of the stacktrace.
	 */
	@Override
	public Supplier<String> get() {
		StackWalker.StackFrame[] stack =
				StackWalker.getInstance()
				           .walk(this);

		if (stack.length == 0) {
			return () -> "";
		}

		if (stack.length == 1) {
			return () -> "\t" + stack[0].toString() + "\n";
		}

		return () -> {
			StringBuilder sb = new StringBuilder();

			for (int j = stack.length - 2; j > 0; j--) {
				StackWalker.StackFrame previous = stack[j];

				if (!full) {
					if (previous.isNativeMethod()) {
						continue;
					}

					String previousRow =
							previous.getClassName() + "." + previous.getMethodName();
					if (shouldSanitize(previousRow)) {
						continue;
					}
				}
				sb.append("\t")
				  .append(previous.toString())
				  .append("\n");
				break;
			}

			sb.append("\t")
			  .append(stack[stack.length - 1].toString())
			  .append("\n");

			return sb.toString();
		};
	}
}