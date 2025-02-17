/*
 * Copyright (c) 2020-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

package io.reactor.gradle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.api.tasks.testing.Test;
import org.gradle.jvm.toolchain.JavaCompiler;
import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainService;

/**
 * @author Simon Baslé
 * @author Dariusz Jędrzejczyk
 */
public class JavaConventions implements Plugin<Project> {

	private final JavaToolchainService javaToolchains;

	@Inject
	JavaConventions(JavaToolchainService javaToolchains) {
		this.javaToolchains = javaToolchains;
	}

	@Override
	public void apply(Project project) {
		project.getPlugins().withType(JavaPlugin.class, plugin -> applyJavaConvention(project));
	}

	private void applyJavaConvention(Project project) {
		project.afterEvaluate(p -> {
			p.getTasks()
			 .withType(JavaCompile.class)
			 .forEach(compileTask -> {
				 compileTask.getJavaCompiler().set(javaToolchains.compilerFor(spec -> spec.getLanguageVersion().set(JavaLanguageVersion.of(21))));

				 compileTask.getOptions().setEncoding("UTF-8");

				 setJavaRelease(compileTask, project);
				 List<String> compilerArgs = new ArrayList<>(compileTask.getOptions()
				                                                        .getCompilerArgs());

				 if (compileTask.getName().endsWith("TestJava")) {
					 compilerArgs.add("-parameters");
				 }
				 compilerArgs.addAll(Arrays.asList(
						 "-Xlint:-varargs",
						 // intentionally disabled
						 "-Xlint:cast",
						 "-Xlint:classfile",
						 "-Xlint:dep-ann",
						 "-Xlint:divzero",
						 "-Xlint:empty",
						 "-Xlint:finally",
						 "-Xlint:overrides",
						 "-Xlint:path",
						 "-Xlint:-processing",
						 "-Xlint:static",
						 "-Xlint:try",
						 "-Xlint:deprecation",
						 "-Xlint:unchecked",
						 "-Xlint:-serial",// intentionally disabled
						 "-Xlint:-options",// intentionally disabled
						 "-Xlint:-fallthrough",// intentionally disabled
						 "-Xmaxerrs", "500",
						 "-Xmaxwarns", "1000"
				 ));

				 compileTask.getOptions().setCompilerArgs(compilerArgs);
			 });
		});

		project.afterEvaluate(p -> {
			p.getTasks()
			 .withType(Test.class)
			 .forEach(testTask -> {
				 int version = testTask.getJavaLauncher().get().getMetadata().getLanguageVersion().asInt();
				 System.out.println(testTask.getName() + ": running with Java " + version);
			 });
		});
	}

	private void setJavaRelease(JavaCompile task, Project project) {
		int defaultVersion = 8;
		int releaseVersion = defaultVersion;
		int compilerVersion = task.getJavaCompiler().get().getMetadata().getLanguageVersion().asInt();

		for (int version = defaultVersion ; version <= compilerVersion ; version++) {
			if (task.getName().contains("Java" + version)) {
				releaseVersion = version;
				break;
			}
		}
		final int finalVersion = releaseVersion;

		System.out.println(task.getName() + ": compiling with Java " + compilerVersion + ", targeting " + finalVersion);

		Provider<JavaCompiler> compiler = project.getExtensions()
		                                         .getByType(JavaToolchainService.class)
		                                         .compilerFor(spec -> spec.getLanguageVersion().set(JavaLanguageVersion.of(finalVersion)));

		// We can't use --release for Java8: https://bugs.openjdk.org/browse/JDK-8206937
		// task.getOptions().getRelease().set(releaseVersion);

		String releaseVersionString = JavaLanguageVersion.of(finalVersion).toString();
		task.setSourceCompatibility(releaseVersionString);
		task.setTargetCompatibility(releaseVersionString);
		FileCollection existingBootClasspath = task.getOptions().getBootstrapClasspath();
		FileCollection bootClasspath = project.files(compiler.map(c -> c.getMetadata().getInstallationPath().getAsFileTree()).get());

		if (existingBootClasspath != null) {
			bootClasspath = existingBootClasspath.plus(bootClasspath);
		}

		task.getOptions().setBootstrapClasspath(bootClasspath);
	}
}
