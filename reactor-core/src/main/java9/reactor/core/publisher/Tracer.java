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
final class Tracer {

	/**
	 * Transform the current stack trace into a {@link String} representation,
	 * each element being prepended with a tabulation and appended with a
	 * newline.
	 *
	 * @param full include all frames if true, sanitize the output otherwise
	 * @return the string version of the stacktrace.
	 */
	static Supplier<String> callSiteSupplier(boolean full) {
		StackFrame[] frames  = StackWalker.getInstance().walk(stack -> {
			StackFrame[] previous = new StackFrame[2];
			stack
					.skip(2) // callSiteSupplier + AssemblySnapshotException.<init>
					.filter(frame -> {
						if (!full && frame.isNativeMethod()) {
							return false;
						}

						String classAndMethod = frame.getClassName() + "." + frame.getMethodName();
						if (!full && Traces.shouldSanitize(classAndMethod)) {
							return false;
						}

						boolean userCode = Traces.isUserCode(classAndMethod);
						if (userCode) {
							previous[1] = frame;
						}
						else {
							previous[0] = frame;
						}
						return userCode;
					})
					.findFirst();

			return previous;
		});

		return () -> {
			StringBuilder sb = new StringBuilder();
			if (frames[0] != null) {
				sb.append("\t").append(frames[0].toString()).append("\n");
			}
			if (frames[1] != null) {
				sb.append("\t").append(frames[1].toString()).append("\n");
			}
			return sb.toString();
		};
	}
}
