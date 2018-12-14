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

import java.util.function.Supplier;

import sun.misc.JavaLangAccess;
import sun.misc.SharedSecrets;

/**
 * Utility class for the call-site extracting.
 *
 * @author Simon Baslé
 * @author Sergei Egorov
 */
final class Tracer {

	private static final JavaLangAccess javaLangAccess = SharedSecrets.getJavaLangAccess();

	/**
	 * Transform the current stack trace into a {@link String} representation,
	 * each element being prepended with a tabulation and appended with a
	 * newline.
	 *
	 * @param full include all frames if true, sanitize the output otherwise
	 * @return the string version of the call-site.
	 */
	static Supplier<String> callSiteSupplier(boolean full) {
		Throwable throwable = new Throwable();
		return () -> {
			int stackTraceDepth = javaLangAccess.getStackTraceDepth(throwable);

			StackTraceElement previousElement = null;
			StackTraceElement userElement = null;
			for (int i = 0; i < stackTraceDepth; i++) {
				StackTraceElement e = javaLangAccess.getStackTraceElement(throwable, i);

				if (!full && e.getLineNumber() <= 1) {
					continue;
				}

				String classAndMethod = e.getClassName() + "." + e.getMethodName();
				if (!full && Traces.shouldSanitize(classAndMethod)) {
					continue;
				}

				if (Traces.isUserCode(classAndMethod)) {
					userElement = e;
					break;
				}
				else {
					previousElement = e;
				}
			}

			StringBuilder sb = new StringBuilder();

			if (previousElement != null) {
				sb.append("\t").append(previousElement.toString()).append("\n");
			}
			if (userElement != null) {
				sb.append("\t").append(userElement.toString()).append("\n");
			}

			return sb.toString();
		};
	}
}
