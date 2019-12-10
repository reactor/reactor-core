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

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

@RunWith(Parameterized.class)
public class CoreSubscriberArchTest {

	@Parameterized.Parameters(name = "{index}: {1}")
	public static Object[][] scanClasses() {
		JavaClasses classes = new ClassFileImporter()
				.withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_ARCHIVES)
				.importPackagesOf(CoreSubscriber.class);

		return classes.stream()
		              .sorted(Comparator.comparing(JavaClass::getName))
		              .map(it -> new Object[] {
		              		classes.that(DescribedPredicate.equalTo(it)),
				            it.getName()
				                .replace("reactor.", "r.")
				                .replace("r.core.", "r.c.")
				                .replace("r.c.publisher.", "r.c.p.")
				                .replace("r.c.scheduler.", "r.c.s.")
		              })
		              .toArray(Object[][]::new);
	}

	final JavaClasses clazz;

	public CoreSubscriberArchTest(JavaClasses clazz, String className) {
		this.clazz = clazz;
	}

	@Test
	public void shouldNotUseDefaultCurrentContext() {
		AtomicBoolean checked = new AtomicBoolean(false);
		classes()
				.that().implement(CoreSubscriber.class)
				.and().doNotHaveModifier(JavaModifier.ABSTRACT)
				.should(new ArchCondition<JavaClass>("not use the default currentContext()") {
					@Override
					public void check(JavaClass item, ConditionEvents events) {
						checked.set(true);
						boolean overriddenMethod = item
								.getAllMethods()
								.stream()
								.filter(it -> "currentContext".equals(it.getName()))
								.filter(it -> it.getRawParameterTypes().isEmpty())
								.anyMatch(it -> !it.getOwner().isEquivalentTo(CoreSubscriber.class));

						if (!overriddenMethod) {
							events.add(SimpleConditionEvent.violated(item, "currentContext() is not overridden"));
						}
					}
				})
				.check(clazz);

		Assume.assumeTrue(checked.get());
	}
}
