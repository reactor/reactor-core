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

import java.util.function.Function;
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

	private static Function<Boolean, Supplier<String>> callSiteSupplierFactory;

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
						Function<Boolean, Supplier<String>> function = (Function) clazz.getDeclaredConstructor()
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

	/**
	 * Transform the current stack trace into a {@link String} representation,
	 * each element being prepended with a tabulation and appended with a
	 * newline.
	 *
	 * @param full include all frames if true, sanitize the output otherwise
	 * @return the string version of the call-site.
	 */
	static Supplier<String> callSiteSupplier(boolean full) {
		return callSiteSupplierFactory.apply(full);
	}

	@SuppressWarnings("unused")
	static class SharedSecretsCallSiteSupplierFactory implements Function<Boolean, Supplier<String>> {

		@Override
		public Supplier<String> apply(Boolean full) {
			return new TracingException(full);
		}

		static class TracingException extends Throwable implements Supplier<String> {

			static final JavaLangAccess javaLangAccess = SharedSecrets.getJavaLangAccess();

			final boolean full;

			TracingException(boolean full) {
				this.full = full;
			}

			@Override
			public String get() {
				int stackTraceDepth = javaLangAccess.getStackTraceDepth(this);

				StackTraceElement previousElement = null;
				StackTraceElement userElement = null;
				// 3 because: apply + callSiteSupplier + AssemblySnapshot.<init>
				for (int i = 3; i < stackTraceDepth; i++) {
					StackTraceElement e = javaLangAccess.getStackTraceElement(this, i);

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
			}
		}
	}

	@SuppressWarnings("unused")
	static class ExceptionCallSiteSupplierFactory implements Function<Boolean, Supplier<String>> {

		@Override
		public Supplier<String> apply(Boolean full) {
			return new TracingException(full);
		}

		static class TracingException extends Throwable implements Supplier<String> {

			final boolean full;

			TracingException(boolean full) {
				this.full = full;
			}

			@Override
			public String get() {
				StackTraceElement previousElement = null;
				StackTraceElement userElement = null;
				StackTraceElement[] stackTrace = getStackTrace();
				// 3 because: apply + callSiteSupplier + AssemblySnapshot.<init>
				for (int i = 3; i < stackTrace.length; i++) {
					StackTraceElement e = stackTrace[i];

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
			}
		}
	}
}
