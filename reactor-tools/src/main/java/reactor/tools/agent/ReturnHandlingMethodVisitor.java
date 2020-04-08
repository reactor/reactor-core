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
 * Checkpoints every method returning Mono/Flux/ParallelFlux,
 * unless there were operator calls in the method body.
 * Before:
 * <pre>
 *     public Mono<String> request() {
 *         return MyWebClient.get("/foo").bodyToMono(String.class);
 *     }
 * </pre>
 * After:
 * <pre>
 *     public Mono<String> request() {
 *         return MyWebClient.get("/foo").bodyToMono(String.class).checkpoint("MyClass.request(MyClass.java:123)");
 *     }
 * </pre>
 *
 */
class ReturnHandlingMethodVisitor extends MethodVisitor {

    final AtomicBoolean changed;

    final String currentClassName;

    final String currentMethod;

    final String returnType;

    final String currentSource;

    int currentLine = -1;

    boolean checkpointed = false;

    ReturnHandlingMethodVisitor(
            MethodVisitor visitor,
            String returnType,
            String currentClassName,
            String currentMethod,
            String currentSource,
            AtomicBoolean changed
    ) {
        super(Opcodes.ASM7, visitor);
        this.changed = changed;
        this.currentClassName = currentClassName;
        this.currentMethod = currentMethod;
        this.returnType = returnType;
        this.currentSource = currentSource;
    }

    @Override
    public void visitLineNumber(int line, Label start) {
        super.visitLineNumber(line, start);
        currentLine = line;
    }

    @Override
    public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface) {
        super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);

        if (!checkpointed && CallSiteInfoAddingMethodVisitor.isCorePublisher(owner)) {
            String returnType = Type.getReturnType(descriptor).getInternalName();
            if (returnType.startsWith("reactor/core/publisher/")) {
                checkpointed = true;
            }
        }
    }

    @Override
    public void visitInsn(int opcode) {
        if (!checkpointed && Opcodes.ARETURN == opcode) {
            changed.set(true);

            String callSite = String.format(
                    "at %s.%s(%s:%d)",
                    currentClassName.replace("/", "."), currentMethod, currentSource, currentLine
            );
            super.visitLdcInsn(callSite);

            super.visitMethodInsn(
                    Opcodes.INVOKESTATIC,
                    "reactor/core/publisher/Hooks",
                    "addReturnInfo",
                    "(Lorg/reactivestreams/Publisher;Ljava/lang/String;)Lorg/reactivestreams/Publisher;",
                    false
            );
            super.visitTypeInsn(Opcodes.CHECKCAST, returnType);
        }

        super.visitInsn(opcode);
    }
}
