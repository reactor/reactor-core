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

import net.bytebuddy.asm.AsmVisitorWrapper;
import net.bytebuddy.build.Plugin;
import net.bytebuddy.description.field.FieldDescription;
import net.bytebuddy.description.field.FieldList;
import net.bytebuddy.description.method.MethodList;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.jar.asm.ClassVisitor;
import net.bytebuddy.pool.TypePool;
import net.bytebuddy.jar.asm.ClassWriter;

public class ReactorDebugByteBuddyPlugin implements Plugin {

	@Override
	public boolean matches(TypeDescription target) {
		return true;
	}

	@Override
	public DynamicType.Builder<?> apply(
			DynamicType.Builder<?> builder,
			TypeDescription typeDescription,
			ClassFileLocator classFileLocator
	) {
		return builder.visit(new AsmVisitorWrapper() {
			@Override
			public int mergeWriter(int flags) {
				return flags | ClassWriter.COMPUTE_MAXS;
			}

			@Override
			public int mergeReader(int flags) {
				return flags;
			}

			@Override
			public ClassVisitor wrap(
					TypeDescription instrumentedType,
					ClassVisitor classVisitor,
					Implementation.Context implementationContext,
					TypePool typePool,
					FieldList<FieldDescription.InDefinedShape> fields,
					MethodList<?> methods,
					int writerFlags,
					int readerFlags
			) {
				return new ReactorDebugClassVisitor(classVisitor, new AtomicBoolean());
			}
		});
	}

	@Override
	public void close() {

	}
}
