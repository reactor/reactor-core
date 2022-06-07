/*
 * Copyright (c) 2019-2022 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.junit.jupiter.api.Test;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static org.assertj.core.api.Assertions.assertThat;

class FuseableBestPracticesArchTest {

	static JavaClasses OPERATOR_CLASSES = new ClassFileImporter()
			.withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
			.withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_JARS)
			.importPackages("reactor.core.publisher");

	@Test
	void smokeTestWhereClassesLoaded() {
		assertThat(OPERATOR_CLASSES).isNotEmpty();
	}

	@Test
	void coreFuseableSubscribersShouldNotExtendNonFuseableOnNext() {
		classes()
				.that().implement(CoreSubscriber.class)
				.and().doNotHaveModifier(JavaModifier.ABSTRACT)
				.and().areAssignableTo(Fuseable.QueueSubscription.class)
				.should(new ArchCondition<JavaClass>("have onNext defined in a Fuseable-compatible way") {
					@Override
					public void check(JavaClass item, ConditionEvents events) {
						boolean overridesMethod = item
								.getAllMethods()
								.stream()
								.filter(it -> "onNext".equals(it.getName()))
								.anyMatch(it -> it.getOwner().isAssignableTo(Fuseable.QueueSubscription.class));

						if (!overridesMethod) {
							events.add(SimpleConditionEvent.violated(
									item,
									item.getFullName() + item.getSourceCodeLocation() + ": onNext(T) is not overridden from a QueueSubscription implementation"
							));
						}
					}
				})
				.check(OPERATOR_CLASSES);
	}
}
