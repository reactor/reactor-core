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

package reactor.core;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;

import org.junit.jupiter.api.Test;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.SynchronousSink;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static org.assertj.core.api.Assertions.assertThat;

class ContextBestPracticesArchTest {

	static JavaClasses CORE_SUBSCRIBER_CLASSES = new ClassFileImporter()
			.withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
			.withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_JARS)
			.importPackagesOf(CoreSubscriber.class);

	// This is ok as this class tests the deprecated FluxProcessor. Will be removed with it in 3.5.
	@SuppressWarnings("deprecation")
	static JavaClasses FLUXPROCESSOR_CLASSES = new ClassFileImporter()
			.withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
			.withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_JARS)
			.importPackagesOf(reactor.core.publisher.FluxProcessor.class);

	@Test
	void smokeTestWhereClassesLoaded() {
		assertThat(CORE_SUBSCRIBER_CLASSES).isNotEmpty();
	}

	@Test
	void corePublishersShouldNotUseDefaultCurrentContext() {
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
				.check(CORE_SUBSCRIBER_CLASSES);
	}

	@Test
	// This is ok as this class tests the deprecated FluxProcessor. Will be removed with it in 3.5.
	@SuppressWarnings("deprecation")
	void fluxProcessorsShouldNotUseDefaultCurrentContext() {
		classes()
				.that().areAssignableTo(reactor.core.publisher.FluxProcessor.class)
				.and().doNotHaveModifier(JavaModifier.ABSTRACT)
				.should(new ArchCondition<JavaClass>("not use the default currentContext()") {
					@Override
					public void check(JavaClass item, ConditionEvents events) {
						boolean overridesMethod = item
								.getAllMethods()
								.stream()
								.filter(it -> "currentContext".equals(it.getName()))
								.filter(it -> it.getRawParameterTypes().isEmpty())
								//method declared in a class derived from FluxProcessor but NOT FluxProcessor itself !
								.anyMatch(it -> it.getOwner().isAssignableTo(reactor.core.publisher.FluxProcessor.class)
										&& !it.getOwner().isEquivalentTo(reactor.core.publisher.FluxProcessor.class));


						if (!overridesMethod) {
							events.add(SimpleConditionEvent.violated(
									item,
									item.getFullName() + item.getSourceCodeLocation() + ": FluxProcessor#currentContext() is not overridden"
							));
						}
					}
				})
				.check(FLUXPROCESSOR_CLASSES);
	}

	@Test
	void oldSinksShouldNotUseDefaultCurrentContext() {
		classes()
			.that().implement(SynchronousSink.class)
			.or().implement(FluxSink.class)
			.or().implement(MonoSink.class)
			.and().doNotHaveModifier(JavaModifier.ABSTRACT)
			.should(new ArchCondition<JavaClass>("not use the default contextView()") {
				@Override
				public void check(JavaClass item, ConditionEvents events) {
					boolean overridesMethod = item
						.getAllMethods()
						.stream()
						.filter(it -> "contextView".equals(it.getName()))
						.filter(it -> it.getRawParameterTypes().isEmpty())
						.anyMatch(it -> !it.getOwner().isEquivalentTo(SynchronousSink.class)
							&& !it.getOwner().isEquivalentTo(FluxSink.class)
							&& !it.getOwner().isEquivalentTo(MonoSink.class)
						);

					if (!overridesMethod) {
						events.add(SimpleConditionEvent.violated(
							item,
							item.getFullName() + item.getSourceCodeLocation() + ": contextView() is not overridden"
						));
					}
				}
			})
			.check(CORE_SUBSCRIBER_CLASSES);
	}
}
