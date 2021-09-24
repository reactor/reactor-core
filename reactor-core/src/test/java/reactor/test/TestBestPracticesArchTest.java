/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.test;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaAnnotation;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;

/**
 * @author Simon Baslé
 */
public class TestBestPracticesArchTest {

	static JavaClasses TEST_CLASSES = new ClassFileImporter()
		.withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_JARS)
		.withImportOption(location -> !ImportOption.Predefined.DO_NOT_INCLUDE_TESTS.includes(location))
		.importPackages("reactor");

	@Test
	void parameterizedTestsIncludeDisplayNameInPattern() {
		DescribedPredicate<JavaAnnotation<?>> parameterizedTestWithDisplayName =
			DescribedPredicate.describe("ParameterizedTest with {displayName}, or perhaps consider @ParameterizedTestWithName instead",
				javaAnnotation -> {
					if (!javaAnnotation.getRawType().isAssignableFrom(ParameterizedTest.class)) {
						return false;
					}
					return javaAnnotation.get("name").map(String::valueOf).orElse("NOPE")
						.contains(ParameterizedTest.DISPLAY_NAME_PLACEHOLDER);
				});

		methods()
			.that().areAnnotatedWith(ParameterizedTest.class)
			.should().beAnnotatedWith(parameterizedTestWithDisplayName)
			.check(TEST_CLASSES);
	}
}
