/*
 * Copyright (c) 2011-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.reactor.gradle;

import java.util.Arrays;
import java.util.Set;

import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.jupiter.api.Test;

import static io.reactor.gradle.DetectCiPlugin.DETECTED_EXTENSION;
import static io.reactor.gradle.DetectCiPlugin.FLAG_EXTENSION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
class DetectCiPluginTest {

	@Test
	void ignoresSubprojects() {
		DetectCiPlugin plugin = new DetectCiPlugin(Arrays.asList("CI", "CONTINUOUS_INTEGRATION", "TRAVIS", "CIRCLECI", "bamboo_planKey", "GITHUB_ACTION"));
		Project root = ProjectBuilder.builder().withName("root").build();
		Project sub = ProjectBuilder.builder().withName("sub").withParent(root).build();

		plugin.apply(sub);

		assertThat(sub.getExtensions().findByName(FLAG_EXTENSION)).as(FLAG_EXTENSION).isNull();
		assertThat(sub.getExtensions().findByName(DETECTED_EXTENSION)).as(DETECTED_EXTENSION).isNull();
	}

	@Test
	void detectsGenericCi() {
		DetectCiPlugin plugin = new DetectCiPlugin(Arrays.asList("CI", "NOISE"));
		Project root = ProjectBuilder.builder().withName("root").build();

		plugin.apply(root);

		assertThat(root.getExtensions().getByName(FLAG_EXTENSION))
				.as(FLAG_EXTENSION)
				.isInstanceOfSatisfying(Boolean.class, b -> assertThat(b).isTrue());
		assertThat(root.getExtensions().getByName(DETECTED_EXTENSION))
				.as(DETECTED_EXTENSION)
				.isInstanceOfSatisfying(Set.class, s -> assertThat(s).containsExactly("CI"));
	}

	@Test
	void detectsGenericContinuousIntegration() {
		DetectCiPlugin plugin = new DetectCiPlugin(Arrays.asList("CONTINUOUS_INTEGRATION", "NOISE"));
		Project root = ProjectBuilder.builder().withName("root").build();

		plugin.apply(root);

		assertThat(root.getExtensions().getByName(FLAG_EXTENSION))
				.as(FLAG_EXTENSION)
				.isInstanceOfSatisfying(Boolean.class, b -> assertThat(b).isTrue());
		assertThat(root.getExtensions().getByName(DETECTED_EXTENSION))
				.as(DETECTED_EXTENSION)
				.isInstanceOfSatisfying(Set.class, s -> assertThat(s).containsExactly("CONTINUOUS_INTEGRATION"));
	}

	@Test
	void detectsTravis() {
		DetectCiPlugin plugin = new DetectCiPlugin(Arrays.asList("TRAVIS", "NOISE"));
		Project root = ProjectBuilder.builder().withName("root").build();

		plugin.apply(root);

		assertThat(root.getExtensions().getByName(FLAG_EXTENSION))
				.as(FLAG_EXTENSION)
				.isInstanceOfSatisfying(Boolean.class, b -> assertThat(b).isTrue());
		assertThat(root.getExtensions().getByName(DETECTED_EXTENSION))
				.as(DETECTED_EXTENSION)
				.isInstanceOfSatisfying(Set.class, s -> assertThat(s).containsExactly("TRAVIS"));
	}

	@Test
	void detectsCircleCi() {
		DetectCiPlugin plugin = new DetectCiPlugin(Arrays.asList("CIRCLECI", "NOISE"));
		Project root = ProjectBuilder.builder().withName("root").build();

		plugin.apply(root);

		assertThat(root.getExtensions().getByName(FLAG_EXTENSION))
				.as(FLAG_EXTENSION)
				.isInstanceOfSatisfying(Boolean.class, b -> assertThat(b).isTrue());
		assertThat(root.getExtensions().getByName(DETECTED_EXTENSION))
				.as(DETECTED_EXTENSION)
				.isInstanceOfSatisfying(Set.class, s -> assertThat(s).containsExactly("CIRCLECI"));
	}

	@Test
	void detectsBamboo() {
		DetectCiPlugin plugin = new DetectCiPlugin(Arrays.asList("bamboo_planKey", "NOISE"));
		Project root = ProjectBuilder.builder().withName("root").build();

		plugin.apply(root);

		assertThat(root.getExtensions().getByName(FLAG_EXTENSION))
				.as(FLAG_EXTENSION)
				.isInstanceOfSatisfying(Boolean.class, b -> assertThat(b).isTrue());
		assertThat(root.getExtensions().getByName(DETECTED_EXTENSION))
				.as(DETECTED_EXTENSION)
				.isInstanceOfSatisfying(Set.class, s -> assertThat(s).containsExactly("bamboo_planKey"));
	}

	@Test
	void detectsGithubActions() {
		DetectCiPlugin plugin = new DetectCiPlugin(Arrays.asList("GITHUB_ACTION", "NOISE"));
		Project root = ProjectBuilder.builder().withName("root").build();

		plugin.apply(root);

		assertThat(root.getExtensions().getByName(FLAG_EXTENSION))
				.as(FLAG_EXTENSION)
				.isInstanceOfSatisfying(Boolean.class, b -> assertThat(b).isTrue());
		assertThat(root.getExtensions().getByName(DETECTED_EXTENSION))
				.as(DETECTED_EXTENSION)
				.isInstanceOfSatisfying(Set.class, s -> assertThat(s).containsExactly("GITHUB_ACTION"));
	}

}