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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utilities around manipulating stack traces and displaying assembly traces.
 *
 * @author Simon Baslé
 */
final class Traces {

	/**
	 * Return true for strings (usually from a stack trace element) that should be
	 * sanitized out by {@link Tracer#callSiteSupplier(boolean)}.
	 *
	 * @param stackTraceRow the row to check
	 * @return true if it should be sanitized out, false if it should be kept
	 */
	static boolean shouldSanitize(String stackTraceRow) {
		return stackTraceRow.trim().isEmpty()
				|| stackTraceRow.contains("java.util.function")
				|| stackTraceRow.contains("reactor.core.publisher.Mono.onAssembly")
				|| stackTraceRow.contains("reactor.core.publisher.Flux.onAssembly")
				|| stackTraceRow.contains("reactor.core.publisher.ParallelFlux.onAssembly")
				|| stackTraceRow.contains("reactor.core.publisher.SignalLogger")
				|| stackTraceRow.contains("FluxOnAssembly.")
				|| stackTraceRow.contains("FluxOnAssembly$AssemblySnapshot")
				|| stackTraceRow.contains("MonoOnAssembly.")
				|| stackTraceRow.contains("MonoCallableOnAssembly.")
				|| stackTraceRow.contains("FluxCallableOnAssembly.")
				|| stackTraceRow.contains("OnOperatorDebug")
				|| stackTraceRow.contains("reactor.core.publisher.Hooks")
				|| stackTraceRow.contains(".junit.runner")
				|| stackTraceRow.contains(".junit4.runner")
				|| stackTraceRow.contains(".junit.internal")
				|| stackTraceRow.contains("org.gradle.")
				|| stackTraceRow.contains("sun.reflect")
				|| stackTraceRow.contains("useTraceAssembly")
				|| stackTraceRow.contains("java.lang.Thread.")
				|| stackTraceRow.contains("ThreadPoolExecutor")
				|| stackTraceRow.contains("org.apache.catalina.")
				|| stackTraceRow.contains("org.apache.tomcat.")
				|| stackTraceRow.contains("com.intellij.")
				|| stackTraceRow.contains("java.lang.reflect");
	}

	/**
	 * Extract operator information out of an assembly stack trace in {@link String} form
	 * (see {@link Tracer#callSiteSupplier(boolean)}).
	 * <p>
	 * Most operators will result in a line of the form {@code "Flux.map ⇢ user.code.Class.method(Class.java:123)"},
	 * that is:
	 * <ol>
	 *     <li>The top of the stack is inspected for Reactor API references, and the deepest
	 *     one is kept, since multiple API references generally denote an alias operator.
	 *     (eg. {@code "Flux.map"})</li>
	 *     <li>The next stacktrace element is considered user code and is appended to the
	 *     result with a {@code ⇢} separator. (eg. {@code " ⇢ user.code.Class.method(Class.java:123)"})</li>
	 *     <li>If no user code is found in the sanitized stack, then the API reference is outputed in the later format only.</li>
	 *     <li>If the sanitized stack is empty, returns {@code "[no operator assembly information]"}</li>
	 * </ol>
	 *
	 *
	 * @param source the sanitized assembly stacktrace in String format.
	 * @return a {@link String} representing operator and operator assembly site extracted
	 * from the assembly stack trace.
	 */
	static String extractOperatorAssemblyInformation(String source) {
		return extractOperatorAssemblyInformation(source, false);
	}

	static boolean isUserCode(String line) {
		return !line.startsWith("reactor.core.publisher") || line.contains("Test");
	}

	/**
	 * Extract operator information out of an assembly stack trace in {@link String} form
	 * (see {@link Tracer#callSiteSupplier(boolean)}) which potentially
	 * has a header line that one can skip by setting {@code skipFirst} to {@code true}.
	 * <p>
	 * Most operators will result in a line of the form {@code "Flux.map ⇢ user.code.Class.method(Class.java:123)"},
	 * that is:
	 * <ol>
	 *     <li>The top of the stack is inspected for Reactor API references, and the deepest
	 *     one is kept, since multiple API references generally denote an alias operator.
	 *     (eg. {@code "Flux.map"})</li>
	 *     <li>The next stacktrace element is considered user code and is appended to the
	 *     result with a {@code ⇢} separator. (eg. {@code " ⇢ user.code.Class.method(Class.java:123)"})</li>
	 *     <li>If no user code is found in the sanitized stack, then the API reference is outputed in the later format only.</li>
	 *     <li>If the sanitized stack is empty, returns {@code "[no operator assembly information]"}</li>
	 * </ol>
	 *
	 *
	 * @param source the sanitized assembly stacktrace in String format.
	 * @return a {@link String} representing operator and operator assembly site extracted
	 * from the assembly stack trace.
	 */
	static String extractOperatorAssemblyInformation(String source, boolean skipFirst) {
		String[] uncleanTraces = source.split("\n");
		final List<String> traces = Stream.of(uncleanTraces)
		                                  .map(String::trim)
		                                  .filter(s -> !s.isEmpty())
		                                  .skip(skipFirst ? 1 : 0)
		                                  .collect(Collectors.toList());

		if (traces.isEmpty()) {
			return "[no operator assembly information]";
		}

		int i = 0;
		while (i < traces.size() && !isUserCode(traces.get(i))) {
			i++;
		}

		String apiLine;
		String userCodeLine;
		if (i == 0) {
			//no line was a reactor API line
			apiLine = "";
			userCodeLine = traces.get(0);
		}
		else if (i == traces.size()) {
			//we skipped ALL lines, meaning they're all reactor API lines. We'll fully display the last one
			apiLine = "";
			userCodeLine = traces.get(i-1).replaceFirst("reactor.core.publisher.", "");
		}
		else {
			//currently on user code line, previous one is API
			apiLine = traces.get(i - 1);
			userCodeLine = traces.get(i);
		}

		//now we want something in the form "Flux.map ⇢ user.code.Class.method(Class.java:123)"
		if (apiLine.isEmpty()) return userCodeLine;

		int linePartIndex = apiLine.indexOf('(');
		if (linePartIndex > 0) {
			apiLine = apiLine.substring(0, linePartIndex);
		}
		apiLine = apiLine.replaceFirst("reactor.core.publisher.", "");

		return apiLine + " ⇢ " + userCodeLine;
	}

}
