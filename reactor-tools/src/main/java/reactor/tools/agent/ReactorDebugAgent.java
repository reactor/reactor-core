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
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class ReactorDebugAgent {

	private static Instrumentation instrumentation;

	public static synchronized void init() {
		if (instrumentation != null) {
			return;
		}
		instrumentation = ByteBuddyAgent.install();

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
				ClassVisitor classVisitor = new ClassVisitor(Opcodes.ASM7, cw) {

					private String currentClassName = "";

					private String currentSource = "";

					@Override
					public void visitSource(String source, String debug) {
						super.visitSource(source, debug);
						currentSource = source;
					}

					@Override
					public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
						super.visit(version, access, name, signature, superName, interfaces);
						currentClassName = name;
					}

					@Override
					public MethodVisitor visitMethod(int access, String currentMethod, String descriptor, String signature, String[] exceptions) {
						MethodVisitor visitor = super.visitMethod(access, currentMethod, descriptor, signature, exceptions);

						if (currentClassName.contains("CGLIB$$")) {
							return visitor;
						}

						String returnType = Type.getReturnType(descriptor).getInternalName();
						switch (returnType) {
							// Handle every core publisher type.
							// Note that classes like `GroupedFlux` or `ConnectableFlux` are not included,
							// because they don't have a type-preserving "checkpoint" method
							case "reactor/core/publisher/Flux":
							case "reactor/core/publisher/Mono":
							case "reactor/core/publisher/ParallelFlux":
								visitor = new ReturnHandlingMethodVisitor(visitor, returnType, currentClassName, currentMethod, currentSource, changed);
						}

						return new CallSiteInfoAddingMethodVisitor(visitor, currentClassName, currentMethod, currentSource, changed);
					}
				};

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
