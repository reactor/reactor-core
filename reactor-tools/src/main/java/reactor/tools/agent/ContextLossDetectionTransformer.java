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
import java.security.ProtectionDomain;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * This {@link ClassFileTransformer} modifies every {Flux, Mono}#{transform,transformDeferred}
 * and wraps the user-provided function with {@link reactor.core.publisher.ContextTrackingFunctionWrapper}.
 *
 */
class ContextLossDetectionTransformer implements ClassFileTransformer {

	@Override
	public byte[] transform(
			ClassLoader loader,
			String className,
			Class<?> classBeingRedefined,
			ProtectionDomain protectionDomain,
			byte[] bytes
	) {
		switch (className) {
			case "reactor/core/publisher/Mono":
			case "reactor/core/publisher/Flux":
				ClassReader cr = new ClassReader(bytes);
				ClassWriter cw = new ClassWriter(cr, ClassWriter.COMPUTE_FRAMES);

				cr.accept(new TransformVisitor(cw), 0);

				return cw.toByteArray();
			default:
				return null;

		}
	}

	static class TransformVisitor extends ClassVisitor {

		public TransformVisitor(ClassWriter cw) {
			super(Opcodes.ASM7, cw);
		}

		@Override
		public MethodVisitor visitMethod(
				int access,
				String name,
				String descriptor,
				String signature,
				String[] exceptions
		) {
			MethodVisitor visitor = super.visitMethod(access, name, descriptor, signature, exceptions);

			switch (name) {
				case "transform":
				case "transformDeferred":
					return new MethodVisitor(Opcodes.ASM7, visitor) {
						@Override
						public void visitCode() {
							super.visitCode();

							super.visitTypeInsn(Opcodes.NEW, "reactor/core/publisher/ContextTrackingFunctionWrapper");
							super.visitInsn(Opcodes.DUP);
							super.visitVarInsn(Opcodes.ALOAD, 1);
							super.visitMethodInsn(Opcodes.INVOKESPECIAL,
									"reactor/core/publisher/ContextTrackingFunctionWrapper",
									"<init>",
									"(Ljava/util/function/Function;)V",
									false);
							super.visitVarInsn(Opcodes.ASTORE, 1);
						}
					};
				default:
					return visitor;
			}
		}
	}
}
