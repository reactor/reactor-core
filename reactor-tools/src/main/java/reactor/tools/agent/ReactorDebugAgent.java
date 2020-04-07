/*
 * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.tools.agent;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.security.ProtectionDomain;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.jar.asm.ClassReader;
import net.bytebuddy.jar.asm.ClassVisitor;
import net.bytebuddy.jar.asm.ClassWriter;

public class ReactorDebugAgent {

	private static final String INSTALLED_PROPERTY = "reactor.tools.agent.installed";

	private static Instrumentation instrumentation;

	/**
	 * This method is a part of the Java Agent contract and should not be
	 * used directly.
	 *
	 * @deprecated to discourage the usage from user's code
	 */
	@Deprecated
	public static void premain(String args, Instrumentation instrumentation) {
		instrument(instrumentation);
		System.setProperty(INSTALLED_PROPERTY, "true");
	}

	public static synchronized void init() {
		if (System.getProperty(INSTALLED_PROPERTY) != null) {
			return;
		}

		if (instrumentation != null) {
			return;
		}
		instrumentation = ByteBuddyAgent.install();

		instrument(instrumentation);
	}

	private static void instrument(Instrumentation instrumentation) {
		ClassFileTransformer transformer = new ClassFileTransformer() {
			@Override
			public byte[] transform(
					ClassLoader loader,
					String className,
					Class<?> clazz,
					ProtectionDomain protectionDomain,
					byte[] bytes
			) {
				if (loader == null) {
					return null;
				}

				if (
						className == null ||
								className.startsWith("java/") ||
								className.startsWith("jdk/") ||
								className.startsWith("sun/") ||
								className.startsWith("com/sun/") ||
								className.startsWith("reactor/core/")
				) {
					return null;
				}

				if (
						clazz != null && (
								clazz.isPrimitive() ||
										clazz.isArray() ||
										clazz.isAnnotation() ||
										clazz.isSynthetic()
						)
				) {
					return null;
				}

				ClassReader cr = new ClassReader(bytes);
				ClassWriter cw = new ClassWriter(cr, ClassWriter.COMPUTE_MAXS);

				AtomicBoolean changed = new AtomicBoolean();
				ClassVisitor classVisitor = new ReactorDebugClassVisitor(cw, changed);

				try {
					cr.accept(classVisitor, 0);
				}
				catch (Throwable e) {
					e.printStackTrace();
					throw e;
				}

				if (!changed.get()) {
					return null;
				}

				return cw.toByteArray();
			}
		};

		instrumentation.addTransformer(transformer, true);
	}

	public static synchronized void processExistingClasses() {
		if (System.getProperty(INSTALLED_PROPERTY) != null) {
			// processExistingClasses is a NOOP when running as an agent
			return;
		}

		if (instrumentation == null) {
			throw new IllegalStateException("Must be initialized first!");
		}

		try {
			Class[] classes = Stream
					.of(instrumentation.getInitiatedClasses(ClassLoader.getSystemClassLoader()))
					.filter(aClass -> {
						try {
							if (aClass.getClassLoader() == null) return false;
							if (aClass.isPrimitive() || aClass.isArray() || aClass.isInterface()) return false;
							if (aClass.isAnnotation() || aClass.isSynthetic()) return false;
							String name = aClass.getName();
							if (name == null) return false;
							if (name.startsWith("[")) return false;
							if (name.startsWith("java.")) return false;
							if (name.startsWith("sun.")) return false;
							if (name.startsWith("com.sun.")) return false;
							if (name.startsWith("jdk.")) return false;
							if (name.startsWith("reactor.core.")) return false;

							// May trigger NoClassDefFoundError, fail fast
							aClass.getConstructors();
						}
						catch (LinkageError e) {
							return false;
						}

						return true;
					})
					.toArray(Class[]::new);

			instrumentation.retransformClasses(classes);
		}
		catch (Throwable e) {
			e.printStackTrace();
			// Some classes fail to re-transform (e.g. worker.org.gradle.internal.UncheckedException)
			// See https://bugs.openjdk.java.net/browse/JDK-8014229
		}
	}

}
