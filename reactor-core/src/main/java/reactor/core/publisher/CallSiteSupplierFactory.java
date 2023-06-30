/*
 * Copyright (c) 2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.function.Supplier;
import java.util.stream.Stream;

import sun.misc.JavaLangAccess;
import sun.misc.SharedSecrets;

import static reactor.core.publisher.Traces.full;
import static reactor.core.publisher.Traces.isUserCode;
import static reactor.core.publisher.Traces.shouldSanitize;

class CallSiteSupplierFactory implements Supplier<Supplier<String>> {

	static final Supplier<Supplier<String>> supplier;

	static {
		String[] strategyClasses = {
				CallSiteSupplierFactory.class.getName() + "$SharedSecretsCallSiteSupplierFactory",
				CallSiteSupplierFactory.class.getName() + "$ExceptionCallSiteSupplierFactory",
		};
		// find one available call-site supplier w.r.t. the jdk version to provide
		// linkage-compatibility between jdk 8 and 9+
		supplier = Stream
				.of(strategyClasses)
				.flatMap(className -> {
					try {
						Class<?> clazz = Class.forName(className);
						@SuppressWarnings("unchecked")
						Supplier<Supplier<String>> function = (Supplier) clazz.getDeclaredConstructor()
						                                                      .newInstance();
						return Stream.of(function);
					}
					// explicitly catch LinkageError to support static code analysis
					// tools detect the attempt at finding out jdk environment
					catch (LinkageError e) {
						return Stream.empty();
					}
					catch (Throwable e) {
						return Stream.empty();
					}
				})
				.findFirst()
				.orElseThrow(() -> new IllegalStateException("Valid strategy not found"));
	}



	@Override
	public Supplier<String> get() {
		return supplier.get();
	}

	@SuppressWarnings("unused")
	static class SharedSecretsCallSiteSupplierFactory implements Supplier<Supplier<String>> {

		static {
			SharedSecrets.getJavaLangAccess();
		}

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
				for (int i = 4; i < stackTraceDepth; i++) {
					StackTraceElement e = javaLangAccess.getStackTraceElement(this, i);

					String className = e.getClassName();
					if (isUserCode(className)) {
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
						if (!full && shouldSanitize(classAndMethod)) {
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
				for (int i = 4; i < stackTrace.length; i++) {
					StackTraceElement e = stackTrace[i];

					String className = e.getClassName();
					if (isUserCode(className)) {
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
						if (!full && shouldSanitize(classAndMethod)) {
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