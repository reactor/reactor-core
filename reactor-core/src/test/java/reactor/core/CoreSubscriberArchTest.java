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

package reactor.core;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.junit.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

public class CoreSubscriberArchTest {

	static JavaClasses classes = new ClassFileImporter()
			.withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
			.withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_JARS)
			.importPackagesOf(CoreSubscriber.class);

	@Test
	public void shouldNotUseDefaultCurrentContext() {
		classes()
				.that().implement(CoreSubscriber.class)
				.and().doNotHaveModifier(JavaModifier.ABSTRACT)
				.should(new ArchCondition<JavaClass>("not use the default currentContext()") {
					@Override
					public void check(JavaClass item, ConditionEvents events) {
						boolean overridesMethod = item
								.getAllMethods()
								.stream()
								.filter(it -> "currentContext".equals(it.getName()))
								.filter(it -> it.getRawParameterTypes().isEmpty())
								.anyMatch(it -> !it.getOwner().isEquivalentTo(CoreSubscriber.class));

						if (!overridesMethod) {
							events.add(SimpleConditionEvent.violated(
									item,
									item.getFullName() + item.getSourceCodeLocation() + ": currentContext() is not overridden"
							));
						}
					}
				})
				.check(classes);
	}
}
