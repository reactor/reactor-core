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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.gradle.api.JavaVersion;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.tasks.compile.JavaCompile;

/**
 * @author Simon Baslé
 */
public class JavaConventions implements Plugin<Project> {

	@Override
	public void apply(Project project) {
		project.getPlugins().withType(JavaPlugin.class, plugin -> applyJavaConvention(project));
	}

	private void applyJavaConvention(Project project) {
		JavaPluginConvention java = project.getConvention().getPlugin(JavaPluginConvention.class);
		java.setSourceCompatibility(JavaVersion.VERSION_1_8);
		java.setTargetCompatibility(JavaVersion.VERSION_1_8);

		project.getTasks()
		       .withType(JavaCompile.class)
		       .forEach(compileTask -> {
			       compileTask.getOptions().setEncoding("UTF-8");
			       compileTask.getOptions().setCompilerArgs(Arrays.asList(
					       "-Xlint:-varargs", // intentionally disabled
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
						   "-Xmaxerrs", "500",
						   "-Xmaxwarns", "500",
					       "-Xlint:deprecation",
					       "-Xlint:unchecked",
					       "-Xlint:-serial",      // intentionally disabled
					       "-Xlint:-options",     // intentionally disabled
					       "-Xlint:-fallthrough", // intentionally disabled
					       "-Xlint:-rawtypes",     // TODO enable and fix warnings
						   "-Xmaxwarns",
						   "1000"
			       ));
		       });

		if (JavaVersion.current().isJava8Compatible()) {
			project.getTasks().withType(JavaCompile.class, t -> {
				if (t.getName().equalsIgnoreCase("testcompile")) {
					List<String> args = new ArrayList<>(t.getOptions().getCompilerArgs());
					args.add("-parameters");
					t.getOptions().setCompilerArgs(args);
				}
			});
		}

	}
}
