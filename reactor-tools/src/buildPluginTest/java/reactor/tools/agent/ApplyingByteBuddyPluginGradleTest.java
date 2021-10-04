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

package reactor.tools.agent;

import java.io.File;

import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.BuildTask;
import org.gradle.testkit.runner.GradleRunner;
import org.gradle.testkit.runner.TaskOutcome;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Simon Baslé
 */
class ApplyingByteBuddyPluginGradleTest {

	static File testProjectDir;

	@BeforeEach
	void setup() {
		String testProjectPath = System.getProperty("mock-gradle-dir");
		assertNotNull(testProjectPath, "Cannot find testProjectPath, set or verify -Dmock-gradle-dir");
		testProjectDir = new File(testProjectPath);
		assertTrue(testProjectDir.exists() && testProjectDir.isDirectory(), "testProjectDir not created correctly");
	}

	@Test
	void applyingByteBuddyPluginDuringGradleBuild() {
		BuildResult result = GradleRunner.create()
			.withProjectDir(testProjectDir)
			.withDebug(true)
			.withArguments("test", "--info", "--stacktrace")
			.build();

		//the test task in reactor-tools/src/buildPluginTest/resources/mock-gradle/src/test/java/demo/SomeClassTest.java
		//checks that applying the reactor-tool ByteBuddy plugin in Gradle instruments prod code but not test code.

		assertTrue(result.getOutput().contains("test"));
		final BuildTask task = result.task(":test");
		assertNotNull(task);
		assertEquals(TaskOutcome.SUCCESS, task.getOutcome());
	}
}
