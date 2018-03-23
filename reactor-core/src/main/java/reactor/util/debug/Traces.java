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

package reactor.util.debug;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utilities around manipulating stack traces and displaying assembly traces.
 *
 * @author Simon Baslé
 */
public class Traces {

	/**
	 * Transform the {@link StackTraceElement} array from an exception into a {@link String}
	 * representation, each element being prepended with a tabulation and appended with a
	 * newline. No sanitation is performed.
	 *
	 * @param stackTraceElements the array of {@link StackTraceElement} to convert to {@link String}
	 * @return the string version of the stacktrace.
	 */
	public static String stackTraceToString(StackTraceElement[] stackTraceElements) {
		StringBuilder sb = new StringBuilder();
		for (StackTraceElement e : stackTraceElements) {
			String row = e.toString();
			sb.append("\t")
			  .append(row)
			  .append("\n");
		}

		return sb.toString();

	}

	/**
	 * Transform the {@link StackTraceElement} array from an exception into a {@link String}
	 * representation, each element being prepended with a tabulation and appended with a
	 * newline, unless they don't pass the {@link #shouldSanitize(String) sanitation filter}.
	 *
	 * @param stackTraceElements the array of {@link StackTraceElement} to convert to {@link String}
	 * @return the string version of the stacktrace.
	 */
	public static String stackTraceToSanitizedString(StackTraceElement[] stackTraceElements) {
		StringBuilder sb = new StringBuilder();
		for (StackTraceElement e : stackTraceElements) {
			String row = e.toString();

			if (e.getLineNumber() <= 1) {
				continue;
			}
			if (row.contains("java.util.function")) {
				continue;
			}
			if (row.contains("reactor.core.publisher.Mono.onAssembly")) {
				continue;
			}
			if (row.contains("reactor.core.publisher.Flux.onAssembly")) {
				continue;
			}
			if (row.contains("reactor.core.publisher.ParallelFlux.onAssembly")) {
				continue;
			}
			if (row.contains("reactor.core.publisher.SignalLogger")) {
				continue;
			}
			if (row.contains("FluxOnAssembly.")) {
				continue;
			}
			if (row.contains("MonoOnAssembly.")) {
				continue;
			}
			if (row.contains("MonoCallableOnAssembly.")) {
				continue;
			}
			if (row.contains("FluxCallableOnAssembly.")) {
				continue;
			}
			if (row.contains("OnOperatorDebug")) {
				continue;
			}
			if (row.contains("reactor.core.publisher.Hooks")) {
				continue;
			}
			if (row.contains(".junit.runner")) {
				continue;
			}
			if (row.contains(".junit4.runner")) {
				continue;
			}
			if (row.contains(".junit.internal")) {
				continue;
			}
			if (row.contains("org.gradle.api.internal")) {
				continue;
			}
			if (row.contains("sun.reflect")) {
				continue;
			}
			if (row.contains("useTraceAssembly")) {
				continue;
			}
			if (row.contains("java.lang.Thread.")) {
				continue;
			}
			if (row.contains("ThreadPoolExecutor")) {
				continue;
			}
			if (row.contains("org.apache.catalina.")) {
				continue;
			}
			if (row.contains("org.apache.tomcat.")) {
				continue;
			}
			if (row.contains("com.intellij.")) {
				continue;
			}
			if (row.contains("java.lang.reflect")) {
				continue;
			}

			sb.append("\t")
			  .append(row)
			  .append("\n");
		}

		return sb.toString();
	}

	/**
	 * Strip an operator name of various prefixes and suffixes.
	 * @param name the operator name, usually simpleClassName or fully-qualified classname.
	 * @return the stripped operator name
	 */
	public static final String stripOperatorName(String name) {
		if (name.contains("@") && name.contains("$")) {
			name = name.substring(0, name.indexOf('$'));
			name = name.substring(name.lastIndexOf('.') + 1);
		}
		String stripped = name
				.replaceAll("Parallel|Flux|Mono|Publisher|Subscriber", "")
				.replaceAll("Fuseable|Operator|Conditional", "");

		if(stripped.length() > 0) {
			return stripped.substring(0, 1).toLowerCase() + stripped.substring(1);
		}
		return stripped;
	}

	/**
	 * Extract operator information out of an assembly stack trace in {@link String} form
	 * (see {@link #stackTraceToSanitizedString(StackTraceElement[])}).
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
	public static String extractOperatorAssemblyInformation(String source) {
		return extractOperatorAssemblyInformation(source, false);
	}

	/**
	 * Extract operator information out of an assembly stack trace in {@link String} form
	 * (see {@link #stackTraceToSanitizedString(StackTraceElement[])}) which potentially
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
	public static String extractOperatorAssemblyInformation(String source, boolean skipFirst) {
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
		while (i < traces.size() && traces.get(i).startsWith("reactor.core.publisher") && !traces.get(i).contains("Test")) {
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
			userCodeLine = traces.get(i-1);
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
