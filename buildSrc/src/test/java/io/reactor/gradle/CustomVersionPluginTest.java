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

import io.reactor.gradle.CustomVersionPlugin;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * @author Simon Baslé
 */
public class CustomVersionPluginTest {

	@Test
	void noCustomVersionPropertyDoesNothing() {
		CustomVersionPlugin plugin = new CustomVersionPlugin();
		Project project = ProjectBuilder.builder().withName("foo").build();
		project.setVersion("1.2.3.BUILD-SNAPSHOT");

		plugin.apply(project);

		assertThat(project.getVersion()).hasToString("1.2.3.BUILD-SNAPSHOT");
	}

	@Test
	void acceptsEmptyStringAsNoCustomVersion() {
		CustomVersionPlugin plugin = new CustomVersionPlugin();
		Project project = ProjectBuilder.builder().withName("foo").build();
		project.setVersion("1.2.3.BUILD-SNAPSHOT");
		project.getExtensions().add("versionBranch", "");

		assertThatCode(() -> plugin.apply(project)).doesNotThrowAnyException();
		assertThat(project.getVersion()).hasToString("1.2.3.BUILD-SNAPSHOT");
	}

	@Test
	void rejectSpace() {
		CustomVersionPlugin plugin = new CustomVersionPlugin();
		Project project = ProjectBuilder.builder().withName("foo").build();
		project.setVersion("1.2.3.BUILD-SNAPSHOT");
		project.getExtensions().add("versionBranch", "custom one");

		assertThatExceptionOfType(InvalidUserDataException.class)
				.isThrownBy(() -> plugin.apply(project))
				.withMessage("Custom version for project passed through -PversionBranch must be alphanumeric chars only: custom one");

		assertThat(project.getVersion()).hasToString("1.2.3.BUILD-SNAPSHOT");
	}

	@Test
	void rejectDash() {
		CustomVersionPlugin plugin = new CustomVersionPlugin();
		Project project = ProjectBuilder.builder().withName("foo").build();
		project.setVersion("1.2.3.BUILD-SNAPSHOT");
		project.getExtensions().add("versionBranch", "custom-one");

		assertThatExceptionOfType(InvalidUserDataException.class)
				.isThrownBy(() -> plugin.apply(project))
				.withMessage("Custom version for project passed through -PversionBranch must be alphanumeric chars only: custom-one");

		assertThat(project.getVersion()).hasToString("1.2.3.BUILD-SNAPSHOT");
	}

	@Test
	void rejectUnderscore() {
		CustomVersionPlugin plugin = new CustomVersionPlugin();
		Project project = ProjectBuilder.builder().withName("foo").build();
		project.setVersion("1.2.3.BUILD-SNAPSHOT");
		project.getExtensions().add("versionBranch", "custom_one");

		assertThatExceptionOfType(InvalidUserDataException.class)
				.isThrownBy(() -> plugin.apply(project))
				.withMessage("Custom version for project passed through -PversionBranch must be alphanumeric chars only: custom_one");

		assertThat(project.getVersion()).hasToString("1.2.3.BUILD-SNAPSHOT");
	}

	@Test
	void rejectSpaceEvenIfNotSnapshot() {
		CustomVersionPlugin plugin = new CustomVersionPlugin();
		Project project = ProjectBuilder.builder().withName("foo").build();
		project.setVersion("1.2.3.RELEASE");
		project.getExtensions().add("versionBranch", "custom one");

		assertThatExceptionOfType(InvalidUserDataException.class)
				.isThrownBy(() -> plugin.apply(project))
				.withMessage("Custom version for project passed through -PversionBranch must be alphanumeric chars only: custom one");

		assertThat(project.getVersion()).hasToString("1.2.3.RELEASE");
	}

	@Test
	void rejectDashEvenIfNotSnapshot() {
		CustomVersionPlugin plugin = new CustomVersionPlugin();
		Project project = ProjectBuilder.builder().withName("foo").build();
		project.setVersion("1.2.3.RELEASE");
		project.getExtensions().add("versionBranch", "custom-one");

		assertThatExceptionOfType(InvalidUserDataException.class)
				.isThrownBy(() -> plugin.apply(project))
				.withMessage("Custom version for project passed through -PversionBranch must be alphanumeric chars only: custom-one");

		assertThat(project.getVersion()).hasToString("1.2.3.RELEASE");
	}

	@Test
	void rejectUnderscoreEvenIfNotSnapshot() {
		CustomVersionPlugin plugin = new CustomVersionPlugin();
		Project project = ProjectBuilder.builder().withName("foo").build();
		project.setVersion("1.2.3.RELEASE");
		project.getExtensions().add("versionBranch", "custom_one");

		assertThatExceptionOfType(InvalidUserDataException.class)
				.isThrownBy(() -> plugin.apply(project))
				.withMessage("Custom version for project passed through -PversionBranch must be alphanumeric chars only: custom_one");

		assertThat(project.getVersion()).hasToString("1.2.3.RELEASE");
	}

	@Test
	void doNothingIfPropertyVersionBranchButNotSnapshot() {
		CustomVersionPlugin plugin = new CustomVersionPlugin();
		Project project = ProjectBuilder.builder().withName("foo").build();
		project.setVersion("1.2.3.M1");
		project.getExtensions().add("versionBranch", "custom");

		plugin.apply(project);

		assertThat(project.getVersion()).hasToString("1.2.3.M1");
	}

	@Test
	void changesVersionIfPropertyVersionBranchAndSnapshot() {
		CustomVersionPlugin plugin = new CustomVersionPlugin();
		Project project = ProjectBuilder.builder().withName("foo").build();
		project.setVersion("1.2.3.BUILD-SNAPSHOT");
		project.getExtensions().add("versionBranch", "custom");

		plugin.apply(project);

		assertThat(project.getVersion()).hasToString("1.2.3.custom.BUILD-SNAPSHOT");
	}

}