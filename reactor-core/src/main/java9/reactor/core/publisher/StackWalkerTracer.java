/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.lang.StackWalker.StackFrame;
import java.util.function.Supplier;

/**
 * Utility class for the call-site extracting on Java 9+.
 *
 * @author Sergei Egorov
 */
@SuppressWarnings("unused")
final class StackWalkerTracer implements Supplier<Supplier<String>> {

	/**
	 * Transform the current stack trace into a {@link String} representation,
	 * each element being prepended with a tabulation and appended with a
	 * newline.
	 *
	 * @return the string version of the stacktrace.
	 */
	@Override
	public Supplier<String> get() {
		StackFrame[] stack = StackWalker.getInstance().walk(s -> {
			return s
					.limit(10)
					// get + AssemblySnapshot.<init>
			        .skip(2)
			        .toArray(StackFrame[]::new);
		});

		return () -> {
			for (int i = 0, length = stack.length; i < length; i++) {
				StackFrame frame = stack[i];
				if (frame.isNativeMethod()) {
					continue;
				}

				if (Traces.isUserCode(frame.getClassName())) {
					StringBuilder sb = new StringBuilder();
					for (int j = i - 1; j > 0; j--) {
						StackFrame previous = stack[j];

						if (!Tracer.full) {
							if (previous.isNativeMethod()) {
								continue;
							}

							String previousRow = previous.getClassName() + "." + previous.getMethodName();
							if (Traces.shouldSanitize(previousRow)) {
								continue;
							}
						}
						sb.append("\t")
						  .append(previous.toString())
						  .append("\n");
						break;
					}
					sb.append("\t")
					  .append(frame.toString())
					  .append("\n");
					return sb.toString();
				}
			}
			return "";
		};
	}
}
