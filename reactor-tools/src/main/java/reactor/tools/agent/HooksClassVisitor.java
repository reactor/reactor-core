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

import org.objectweb.asm.*;
import org.objectweb.asm.commons.GeneratorAdapter;

/**
 * Adds the following method to {@link reactor.core.publisher.Hooks},
 * to be called by the call site instrumentation.
 * <pre>
 * public static Publisher addCallSiteInfo(Publisher p, String callSite) {
 *	return Hooks.addAssemblyInfo(
 * 		p,
 * 		new AssemblySnapshot(null, new ConstantSupplier(callSite))
 * 	);
 * }
 * </pre>
 */
class HooksClassVisitor extends ClassVisitor {

    static final String ADD_CALLSITE_INFO_METHOD = "(Lorg/reactivestreams/Publisher;Ljava/lang/String;)Lorg/reactivestreams/Publisher;";
    final ClassWriter cw;

    HooksClassVisitor(ClassWriter cw) {
        super(Opcodes.ASM7, cw);
        this.cw = cw;
    }

    @Override
    public void visitEnd() {
        int access = Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC;
        String methodName = "addCallSiteInfo";
        MethodVisitor addAssemblyInfoMethod = cw.visitMethod(
                access,
                methodName,
                ADD_CALLSITE_INFO_METHOD,
                ADD_CALLSITE_INFO_METHOD,
                new String[0]
        );

        GeneratorAdapter adapter = new GeneratorAdapter(addAssemblyInfoMethod, access, methodName, ADD_CALLSITE_INFO_METHOD);

        adapter.visitCode();
        adapter.loadArg(0);

        adapter.newInstance(Type.getObjectType("reactor/core/publisher/FluxOnAssembly$AssemblySnapshot"));
        adapter.dup();

        adapter.visitInsn(Opcodes.ACONST_NULL);

        adapter.loadArg(1);
        adapter.visitMethodInsn(
                Opcodes.INVOKESTATIC,
                "reactor/tools/agent/ConstantSupplier",
                "of",
                "(Ljava/lang/String;)Lreactor/tools/agent/ConstantSupplier;",
                false
        );
        adapter.visitMethodInsn(
                Opcodes.INVOKESPECIAL,
                "reactor/core/publisher/FluxOnAssembly$AssemblySnapshot",
                "<init>",
                "(Ljava/lang/String;Ljava/util/function/Supplier;)V",
                false
        );
        adapter.visitMethodInsn(
                Opcodes.INVOKESTATIC,
                "reactor/core/publisher/Hooks",
                "addAssemblyInfo",
                "(Lorg/reactivestreams/Publisher;Lreactor/core/publisher/FluxOnAssembly$AssemblySnapshot;)Lorg/reactivestreams/Publisher;",
                false
        );

        adapter.returnValue();
        adapter.endMethod();

        super.visitEnd();
    }
}
