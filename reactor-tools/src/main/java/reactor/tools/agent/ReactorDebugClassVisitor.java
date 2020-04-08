/*
 * Copyright (c) 2020-Present Pivotal Software Inc, All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicBoolean;

import net.bytebuddy.jar.asm.ClassVisitor;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.jar.asm.Type;

class ReactorDebugClassVisitor extends ClassVisitor {

	private final AtomicBoolean changed;

	private String currentClassName;

	private String currentSource;

	ReactorDebugClassVisitor(ClassVisitor classVisitor, AtomicBoolean changed) {
		super(Opcodes.ASM7, classVisitor);
		this.changed = changed;
	}

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
				visitor = new ReturnHandlingMethodVisitor(visitor, returnType, currentClassName, currentMethod, currentSource,
						changed);
		}

		return new CallSiteInfoAddingMethodVisitor(visitor, currentClassName, currentMethod, currentSource, changed);
	}
}
