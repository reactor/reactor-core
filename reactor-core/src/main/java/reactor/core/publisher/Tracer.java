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
import java.util.stream.Stream;

import sun.misc.JavaLangAccess;
import sun.misc.SharedSecrets;

/**
 * Utility class for the call-site extracting.
 *
 * @author Simon Baslé
 * @author Sergei Egorov
 */
final class Tracer {

	/**
	 * If set to true, the creation of FluxOnAssembly will capture the raw stacktrace
	 * instead of the sanitized version.
	 */
	static final boolean full = Boolean.parseBoolean(System.getProperty(
			"reactor.trace.assembly.fullstacktrace",
			"false"));

	/**
	 * Transform the current stack trace into a {@link String} representation,
	 * each element being prepended with a tabulation and appended with a
	 * newline.
	 */
	static Supplier<Supplier<String>> callSiteSupplierFactory;

	static {
		String[] strategyClasses = {
				Tracer.class.getPackage().getName() + ".StackWalkerTracer",
				Tracer.class.getName() + "$SharedSecretsCallSiteSupplierFactory",
				Tracer.class.getName() + "$ExceptionCallSiteSupplierFactory",
		};
		callSiteSupplierFactory = Stream
				.of(strategyClasses)
				.flatMap(className -> {
					try {
						Class<?> clazz = Class.forName(className);
						@SuppressWarnings("unchecked")
						Supplier<Supplier<String>> function = (Supplier) clazz.getDeclaredConstructor()
						                             .newInstance();
						return Stream.of(function);
					}
					catch (Throwable e) {
						return Stream.empty();
					}
				})
				.findFirst()
				.orElseThrow(() -> new IllegalStateException("Valid strategy not found"));
	}

	@SuppressWarnings("unused")
	static class SharedSecretsCallSiteSupplierFactory implements Supplier<Supplier<String>> {

		@Override
		public Supplier<String> get() {
			return new TracingException();
		}

		static class TracingException extends Throwable implements Supplier<String> {

			static final JavaLangAccess javaLangAccess = SharedSecrets.getJavaLangAccess();

			@Override
			public String get() {
				int stackTraceDepth = javaLangAccess.getStackTraceDepth(this);

				StackTraceElement previousElement = null;
				// Skip get()
				for (int i = 2; i < stackTraceDepth; i++) {
					StackTraceElement e = javaLangAccess.getStackTraceElement(this, i);

					String className = e.getClassName();
					if (Traces.isUserCode(className)) {
						StringBuilder sb = new StringBuilder();

						if (previousElement != null) {
							sb.append("\t").append(previousElement.toString()).append("\n");
						}
						sb.append("\t").append(e.toString()).append("\n");
						return sb.toString();
					}
					else {
						if (!full && e.getLineNumber() <= 1) {
							continue;
						}

						String classAndMethod = className + "." + e.getMethodName();
						if (!full && Traces.shouldSanitize(classAndMethod)) {
							continue;
						}
						previousElement = e;
					}
				}

				return "";
			}
		}
	}

	@SuppressWarnings("unused")
	static class ExceptionCallSiteSupplierFactory implements Supplier<Supplier<String>> {

		@Override
		public Supplier<String> get() {
			return new TracingException();
		}

		static class TracingException extends Throwable implements Supplier<String> {

			@Override
			public String get() {
				StackTraceElement previousElement = null;
				StackTraceElement[] stackTrace = getStackTrace();
				// Skip get()
				for (int i = 2; i < stackTrace.length; i++) {
					StackTraceElement e = stackTrace[i];

					String className = e.getClassName();
					if (Traces.isUserCode(className)) {
						StringBuilder sb = new StringBuilder();

						if (previousElement != null) {
							sb.append("\t").append(previousElement.toString()).append("\n");
						}
						sb.append("\t").append(e.toString()).append("\n");
						return sb.toString();
					}
					else {
						if (!full && e.getLineNumber() <= 1) {
							continue;
						}

						String classAndMethod = className + "." + e.getMethodName();
						if (!full && Traces.shouldSanitize(classAndMethod)) {
							continue;
						}
						previousElement = e;
					}
				}

				return "";
			}
		}
	}
}
