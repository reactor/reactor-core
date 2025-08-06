/*
 * Copyright (c) 2018-2024 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;

/**
 * Utilities around manipulating stack traces and displaying assembly traces.
 *
 * @author Simon Baslé
 * @author Sergei Egorov
 */
final class Traces {
	/**
	 * If set to true, the creation of FluxOnAssembly will capture the raw stacktrace
	 * instead of the sanitized version.
	 */
	static final boolean full = Boolean.parseBoolean(System.getProperty(
			"reactor.trace.assembly.fullstacktrace",
			"false"));

	static final String CALL_SITE_GLUE = " ⇢ ";

	/**
	 * Transform the current stack trace into a {@link String} representation,
	 * each element being prepended with a tabulation and appended with a
	 * newline.
	 */
	static final Supplier<Supplier<String>> callSiteSupplierFactory = new CallSiteSupplierFactory();

	private static final String PUBLISHER_PACKAGE_PREFIX = "reactor.core.publisher.";

	/**
	 * Return true for strings (usually from a stack trace element) that should be
	 * sanitized out by {@link Traces#callSiteSupplierFactory}.
	 *
	 * @param stackTraceRow the row to check
	 * @return true if it should be sanitized out, false if it should be kept
	 */
	static boolean shouldSanitize(String stackTraceRow) {
		return stackTraceRow.startsWith("java.util.function")
				|| stackTraceRow.startsWith("reactor.core.publisher.Mono.onAssembly")
				|| stackTraceRow.startsWith("reactor.core.publisher.Flux.onAssembly")
				|| stackTraceRow.startsWith("reactor.core.publisher.ParallelFlux.onAssembly")
				|| stackTraceRow.startsWith("reactor.core.publisher.SignalLogger")
				|| stackTraceRow.startsWith("reactor.core.publisher.FluxOnAssembly")
				|| stackTraceRow.startsWith("reactor.core.publisher.MonoOnAssembly.")
				|| stackTraceRow.startsWith("reactor.core.publisher.MonoCallableOnAssembly.")
				|| stackTraceRow.startsWith("reactor.core.publisher.FluxCallableOnAssembly.")
				|| stackTraceRow.startsWith("reactor.core.publisher.Hooks")
				|| stackTraceRow.startsWith("sun.reflect")
				|| stackTraceRow.startsWith("java.util.concurrent.ThreadPoolExecutor")
				|| stackTraceRow.startsWith("java.lang.reflect");
	}

	/**
	 * Extracts operator information out of an assembly stack trace in {@link String} form
	 * (see {@link Traces#callSiteSupplierFactory}).
	 * <p>
	 * Most operators will result in a line of the form {@code "Flux.map ⇢ user.code.Class.method(Class.java:123)"},
	 * that is:
	 * <ol>
	 *     <li>The top of the stack is inspected for Reactor API references, and the deepest
	 *     one is kept, since multiple API references generally denote an alias operator.
	 *     (eg. {@code "Flux.map"})</li>
	 *     <li>The next stacktrace element is considered user code and is appended to the
	 *     result with a {@code ⇢} separator. (eg. {@code " ⇢ user.code.Class.method(Class.java:123)"})</li>
	 *     <li>If no user code is found in the sanitized stack, then the API reference is output in the later format only.</li>
	 *     <li>If the sanitized stack is empty, returns {@code "[no operator assembly information]"}</li>
	 * </ol>
	 *
	 * @param source the sanitized assembly stacktrace in String format.
	 * @return a {@link String} representing operator and operator assembly site extracted
	 * from the assembly stack trace.
	 */
	static String extractOperatorAssemblyInformation(String source) {
		String[] parts = extractOperatorAssemblyInformationParts(source);
		switch (parts.length) {
			case 0:
				return "[no operator assembly information]";
			case 1:
				return parts[0];
			case 2:
				return parts[0] + CALL_SITE_GLUE + parts[1];
			default:
				throw new IllegalStateException("Unexpected number of assembly info parts: " + parts.length);
		}
	}

	static boolean isUserCode(String line) {
		return !line.startsWith(PUBLISHER_PACKAGE_PREFIX) || line.contains("Test");
	}

	/**
	 * Extracts operator information out of an assembly stack trace in {@link String} array form
	 * (see {@link Traces#callSiteSupplierFactory}).
	 * <p>
	 * The returned array will contain 0, 1 or 2 elements, extracted in a manner as described by
	 * {@link #extractOperatorAssemblyInformation(String)}.
	 *
	 * @param source the sanitized assembly stacktrace in String format.
	 * @return a 0-2 element string array containing the operator and operator assembly site extracted
	 * from the assembly stack trace
	 */
	static String[] extractOperatorAssemblyInformationParts(String source) {
		Iterator<StackLineView> traces = trimmedNonemptyLines(source);

		if (!traces.hasNext()) {
			return new String[0];
		}

		StackLineView prevLine = null;
		StackLineView currentLine = traces.next();

		if (currentLine.isUserCode()) {
			// No line is a Reactor API line.
			return new String[]{currentLine.toString()};
		}

		while (traces.hasNext()) {
			prevLine = currentLine;
			currentLine = traces.next();

			if (currentLine.isUserCode()) {
				// Currently on user code line, previous one is API. Attempt to create something in the form
				// "Flux.map ⇢ user.code.Class.method(Class.java:123)".
				return new String[]{
					prevLine.withoutPublisherPackagePrefix().withoutLocationSuffix().toString(),
					"at " + currentLine};
			}
		}

		// We skipped ALL lines, meaning they're all Reactor API lines. We'll fully display the last
		// one.
		return new String[]{currentLine.withoutPublisherPackagePrefix().toString()};
	}

	/**
	 * Returns an iterator over all trimmed non-empty lines in the given source string.
	 *
	 * @implNote This implementation attempts to minimize allocations.
	 */
	private static Iterator<StackLineView> trimmedNonemptyLines(String source) {
		return new Iterator<StackLineView>() {
			private int           index = 0;
			@Nullable
			private StackLineView next  = getNextLine();

			@Override
			public boolean hasNext() {
				return next != null;
			}

			@Override
			public StackLineView next() {
				StackLineView current = next;
				if (current == null) {
					throw new NoSuchElementException();
				}
				next = getNextLine();
				return current;
			}

			@Nullable
			private StackLineView getNextLine() {
				while (index < source.length()) {
					int end = source.indexOf('\n', index);
					if (end == -1) {
						end = source.length();
					}
					StackLineView line = new StackLineView(source, index, end).trim();
					index = end + 1;
					if (!line.isEmpty()) {
						return line;
					}
				}
				return null;
			}
		};
	}

	/**
	 * Provides optimized access to underlying {@link String} with common operations to
	 * view the stack trace line without unnecessary allocation of temporary
	 * {@code String} objects.
	 */
	static final class StackLineView {

		private final String underlying;
		private final int    start;
		private final int    end;

		StackLineView(String underlying, int start, int end) {
			this.underlying = underlying;
			this.start = start;
			this.end = end;
		}

		StackLineView trim() {
			int newStart = start;
			while (newStart < end && underlying.charAt(newStart) <= ' ') {
				newStart++;
			}
			int newEnd = end;
			while (newEnd > newStart && underlying.charAt(newEnd - 1) <= ' ') {
				newEnd--;
			}
			return newStart == start && newEnd == end ? this : new StackLineView(
					underlying, newStart, newEnd);
		}

		boolean isEmpty() {
			return start == end;
		}

		boolean startsWith(String prefix) {
			boolean canFit = end - start >= prefix.length();
			return canFit && underlying.startsWith(prefix, start);
		}

		boolean contains(String substring) {
			int index = underlying.indexOf(substring, start);
			return index >= start && (index + (substring.length() - 1) < end);
		}

		boolean isUserCode() {
			return !startsWith(PUBLISHER_PACKAGE_PREFIX) || contains("Test");
		}

		StackLineView withoutLocationSuffix() {
			int linePartIndex = underlying.indexOf('(', start);
			return linePartIndex > 0 && linePartIndex < end
				? new StackLineView(underlying, start, linePartIndex)
				: this;
		}

		StackLineView withoutPublisherPackagePrefix() {
			return startsWith(PUBLISHER_PACKAGE_PREFIX)
				? new StackLineView(underlying, start + PUBLISHER_PACKAGE_PREFIX.length(), end)
				: this;
		}

		@Override
		public String toString() {
			return underlying.substring(start, end);
		}
	}
}
