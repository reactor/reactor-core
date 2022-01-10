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

package io.reactor.gradle;

import java.io.File;

import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.JavaExec;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

/**
 * @author Simon Baslé
 */
class JmhExecTask extends JavaExec {

	private static final String DEFAULT_FORMAT = "json";

	@Input
	@Option(option = "include", description="configure bench inclusion using substring")
	private final Property<String> include;

	@Input
	@Option(option = "fullInclude", description = "explicitly configure bench inclusion using full JMH style regexp")
	private final Property<String> fullInclude;

	@Input
	@Option(option = "exclude", description = "explicitly configure bench exclusion using full JMH style regexp")
	private final Property<String> exclude;

	@Input
	@Option(option = "format", description = "configure report format")
	private final Property<String> format;

	@Input
	@Option(option = "profilers", description = "configure jmh profiler(s) to use, comma separated")
	private final Property<String> profilers;

	@Input
	@Option(option = "jvmArgs", description = "configure additional JMH JVM arguments, comma separated")
	private final Property<String> jmhJvmArgs;

	@Input
	@Option(option = "verify", description = "run in verify mode")
	private final Property<String> verify;

	public JmhExecTask() {
		super();
		getMainClass().set("org.openjdk.jmh.Main");
		ObjectFactory objectFactory = this.getObjectFactory();
		include = objectFactory.property(String.class);
		fullInclude = objectFactory.property(String.class);
		exclude = objectFactory.property(String.class);
		format = objectFactory.property(String.class).convention(DEFAULT_FORMAT);
		profilers = objectFactory.property(String.class);
		jmhJvmArgs = objectFactory.property(String.class);
		verify = objectFactory.property(String.class);
	}

	@Optional
	public Property<String> getInclude() {
		return include;
	}

	@Optional
	public Property<String> getFullInclude() {
		return fullInclude;
	}

	@Optional
	public Property<String> getExclude() {
		return exclude;
	}

	@Optional
	public Property<String> getFormat() {
		return format;
	}

	@Optional
	public Property<String> getProfilers() {
		return profilers;
	}

	@Optional
	public Property<String> getJmhJvmArgs() {
		return jmhJvmArgs;
	}

	@Optional
	public Property<String> getVerify() {
		return verify;
	}

	@TaskAction
	public void exec() {
		File resultFile = getProject().file("build/reports/" + getName() + "/result." + format.getOrElse(DEFAULT_FORMAT));

		if (include.isPresent()) {
			args(".*" + include.get() + ".*");
		}
		else if (fullInclude.isPresent()) {
			args(fullInclude.get());
		}

		if(exclude.isPresent()) {
			args("-e", exclude.get());
		}
		if(verify.isPresent()) { // execute benchmarks with the minimum amount of execution (only to check if they are working)
			System.out.println("Running in verify mode");
			args("-f", 1);
			args("-wi", 1);
			args("-i", 1);
		}
		args("-foe", "true"); //fail-on-error
		args("-v", "NORMAL"); //verbosity [SILENT, NORMAL, EXTRA]
		if(profilers.isPresent()) {
			for (String prof : profilers.get().split(",")) {
				args("-prof", prof);
			}
		}
		args("-jvmArgsPrepend", "-Xmx3072m");
		args("-jvmArgsPrepend", "-Xms3072m");
		if(jmhJvmArgs.isPresent()) {
			for(String jvmArg : jmhJvmArgs.get().split(" ")) {
				args("-jvmArgsPrepend", jvmArg);
			}
		}
		args("-rf", format.getOrElse(DEFAULT_FORMAT));
		args("-rff", resultFile);

		System.out.println("\nExecuting JMH with: " + getArgs() + "\n");
		resultFile.getParentFile().mkdirs();

		super.exec();
	}
}
