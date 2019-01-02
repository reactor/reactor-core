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
import java.util.Iterator;
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
			StackFrame[] result = new StackFrame[10];
			Iterator<StackFrame> iterator = s.iterator();
			iterator.next(); // .get

			int i = 0;
			while (iterator.hasNext()) {
				StackFrame frame = iterator.next();

				if (i >= result.length) {
					return new StackFrame[0];
				}

				result[i++] = frame;

				if (Traces.isUserCode(frame.getClassName())) {
					break;
				}
			}
			StackFrame[] copy = new StackFrame[i];
			System.arraycopy(result, 0, copy, 0, i);
			return copy;
		});

		if (stack.length == 0) {
			return () -> "";
		}

		if (stack.length == 1) {
			return () -> "\t" + stack[0].toString() + "\n";
		}

		return () -> {
			StringBuilder sb = new StringBuilder();

			for (int j = stack.length - 2; j > 0; j--) {
				StackFrame previous = stack[j];

				if (!Traces.full) {
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
			  .append(stack[stack.length - 1].toString())
			  .append("\n");

			return sb.toString();
		};
	}
}
