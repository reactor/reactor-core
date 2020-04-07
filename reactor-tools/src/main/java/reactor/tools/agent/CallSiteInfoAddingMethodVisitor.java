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

import net.bytebuddy.jar.asm.Label;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.jar.asm.Type;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Adds callSite info to every operator call (except "checkpoint").
 * Before:
 * <pre>
 *     Flux.just(1)
 *         .map(it -> it)
 * </pre>
 * After:
 * <pre>
 *     Flux flux = Hooks.addCallSiteInfo(Flux.just(1), "Flux.just -> MyClass.myMethod(MyClass.java:12)");
 *     flux = Hooks.addCallSiteInfo(flux.map(it -> it), "Flux.map -> MyClass.myMethod(MyClass.java:13)");
 * </pre>
 *
 */
class CallSiteInfoAddingMethodVisitor extends MethodVisitor {

    static final String ADD_CALLSITE_INFO_METHOD = "(Lorg/reactivestreams/Publisher;Ljava/lang/String;)Lorg/reactivestreams/Publisher;";

    static boolean isCorePublisher(String className) {
        switch (className) {
            case "reactor/core/publisher/Flux":
            case "reactor/core/publisher/Mono":
            case "reactor/core/publisher/ParallelFlux":
            case "reactor/core/publisher/GroupedFlux":
                return true;
            default:
                return false;
        }
    }

    final String currentMethod;

    final String currentClassName;

    final String currentSource;

    final AtomicBoolean changed;

    int currentLine = -1;

    CallSiteInfoAddingMethodVisitor(
            MethodVisitor visitor,
            String currentClassName,
            String currentMethod,
            String currentSource,
            AtomicBoolean changed
    ) {
        super(Opcodes.ASM7, visitor);
        this.currentMethod = currentMethod;
        this.currentClassName = currentClassName;
        this.currentSource = currentSource;
        this.changed = changed;
    }

    @Override
    public void visitLineNumber(int line, Label start) {
        super.visitLineNumber(line, start);
        currentLine = line;
    }

    @Override
    public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface) {
        super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
        if (isCorePublisher(owner)) {
            if ("checkpoint".equals(name)) {
                return;
            }
            String returnType = Type.getReturnType(descriptor).getInternalName();
            if (!returnType.startsWith("reactor/core/publisher/")) {
                return;
            }

            changed.set(true);

            String callSite = String.format(
                    "\t%s.%s\n\t%s.%s(%s:%d)\n",
                    owner.replace("/", "."), name,
                    currentClassName.replace("/", "."), currentMethod, currentSource, currentLine
            );
            super.visitLdcInsn(callSite);
            super.visitMethodInsn(
                    Opcodes.INVOKESTATIC,
                    "reactor/core/publisher/Hooks",
                    "addCallSiteInfo",
                    ADD_CALLSITE_INFO_METHOD,
                    false
            );
            super.visitTypeInsn(Opcodes.CHECKCAST, returnType);
        }
    }
}
