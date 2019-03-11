/*
 * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
import java.lang.instrument.UnmodifiableClassException;
import java.security.ProtectionDomain;

import net.bytebuddy.agent.ByteBuddyAgent;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;

public class ReactorDebugAgent {

	public static void init() {
		Instrumentation instrumentation = ByteBuddyAgent.install();

		ClassFileTransformer transformer = new PublicMethodsClassFileTransformer();
		instrumentation.addTransformer(transformer, true);
		try {
			instrumentation.retransformClasses(Flux.class, Mono.class, ParallelFlux.class);
		}
		catch (UnmodifiableClassException e) {
			throw new RuntimeException(e);
		}
		instrumentation.removeTransformer(transformer);

		transformer = (loader, className, clazz, protectionDomain, bytes) -> {
			if (
					className == null ||
							className.startsWith("java/") ||
							className.startsWith("jdk/") ||
							className.startsWith("sun/") ||
							className.startsWith("reactor/core/")) {
				return bytes;
			}

			ClassReader cr = new ClassReader(bytes);
			ClassWriter cw = new ClassWriter(cr, 0);

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

					return new AssemblyInfoAddingMethodVisitor(visitor, currentMethod, currentClassName, currentSource);
				}
			};

			cr.accept(classVisitor, 0);
			return cw.toByteArray();
		};

		instrumentation.addTransformer(transformer, true);

		for (Class aClass : instrumentation.getAllLoadedClasses()) {
			if (!instrumentation.isModifiableClass(aClass)) {
				continue;
			}
			if (aClass == null || aClass.getClassLoader() == null) {
				continue;
			}
			String name = aClass.getName();
			if (
					name.startsWith("java.") ||
							name.startsWith("sun.") ||
							name.startsWith("jdk.") ||
							name.startsWith("reactor.core.")) {
				continue;
			}

			try {
				instrumentation.retransformClasses(aClass);
			}
			catch (Throwable e) {
				// Some classes fail to re-transform
			}
		}
	}

	static class AssemblyInfoAddingMethodVisitor extends MethodVisitor {

		private final String currentMethod;

		private final String currentClassName;

		private final String currentSource;

		private int currentLine = -1;

		AssemblyInfoAddingMethodVisitor(MethodVisitor visitor, String currentMethod, String currentClassName, String currentSource) {
			super(Opcodes.ASM7, visitor);
			this.currentMethod = currentMethod;
			this.currentClassName = currentClassName;
			this.currentSource = currentSource;
		}

		@Override
		public void visitLineNumber(int line, Label start) {
			super.visitLineNumber(line, start);
			currentLine = line;
		}

		@Override
		public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface) {
			if (!"onAssemblyInfo".equals(name) && !"<init>".equals(name) && !"subscribe".equals(name)) {
				switch (owner) {
					case "reactor/core/publisher/Flux":
					case "reactor/core/publisher/Mono":
					case "reactor/core/publisher/ParallelFlux":
						String returnType = descriptor.substring(descriptor.lastIndexOf(")") + 1);
						if (!returnType.startsWith("Lreactor/core/publisher/")) {
							return;
						}

						String callSite = String.format(
								"%s.%s\n%s.%s(%s:%d)",
								owner.replace("/", "."), name,
								currentClassName.replace("/", "."),
								currentMethod, currentSource, currentLine
						);
						super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
						super.visitLdcInsn(callSite);
						super.visitMethodInsn(Opcodes.INVOKEVIRTUAL, returnType, "onAssemblyInfo", "(Ljava/lang/String;)" + returnType, false);
						break;
					default:
						super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
				}
			} else {
				super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
			}
		}
	}

	static class PublicMethodsClassFileTransformer implements ClassFileTransformer {

		@Override
		public byte[] transform(
				ClassLoader loader,
				String className,
				Class<?> clazz,
				ProtectionDomain protectionDomain,
				byte[] bytes
		) {
			switch (className) {
				case "reactor/core/publisher/Flux":
				case "reactor/core/publisher/Mono":
				case "reactor/core/publisher/ParallelFlux":
					ClassReader cr = new ClassReader(bytes);
					ClassWriter cw = new ClassWriter(cr, 0);

					ClassVisitor classVisitor = new ClassVisitor(Opcodes.ASM7, cw) {
						@Override
						public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
							if ("onAssemblyInfo".equals(name)) {
								access &= ~Opcodes.ACC_PROTECTED;
								access |= Opcodes.ACC_PUBLIC;
							}
							return super.visitMethod(access, name, descriptor, signature, exceptions);
						}
					};

					cr.accept(classVisitor, 0);
					return cw.toByteArray();
			}
			return bytes;
		}
	}
}
