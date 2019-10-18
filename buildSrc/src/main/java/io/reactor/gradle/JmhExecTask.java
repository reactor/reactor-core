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

import java.io.File;

import org.gradle.api.tasks.JavaExec;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

/**
 * @author Simon Baslé
 */
class JmhExecTask extends JavaExec {

	private String include;
	private String fullInclude;
	private String exclude;
	private String format = "json";
	private String profilers;
	private String jmhJvmArgs;
	private String verify;

	public JmhExecTask() {
		super();
	}

	public String getInclude() {
		return include;
	}

	@Option(option = "include", description="configure bench inclusion using substring")
	public void setInclude(String include) {
		this.include = include;
	}

	public String getFullInclude() {
		return fullInclude;
	}

	@Option(option = "fullInclude", description = "explicitly configure bench inclusion using full JMH style regexp")
	public void setFullInclude(String fullInclude) {
		this.fullInclude = fullInclude;
	}

	public String getExclude() {
		return exclude;
	}

	@Option(option = "exclude", description = "explicitly configure bench exclusion using full JMH style regexp")
	public void setExclude(String exclude) {
		this.exclude = exclude;
	}

	public String getFormat() {
		return format;
	}

	@Option(option = "format", description = "configure report format")
	public void setFormat(String format) {
		this.format = format;
	}

	public String getProfilers() {
		return profilers;
	}

	@Option(option = "profilers", description = "configure jmh profiler(s) to use, comma separated")
	public void setProfilers(String profilers) {
		this.profilers = profilers;
	}

	public String getJmhJvmArgs() {
		return jmhJvmArgs;
	}

	@Option(option = "jvmArgs", description = "configure additional JMH JVM arguments, comma separated")
	public void setJmhJvmArgs(String jvmArgs) {
		this.jmhJvmArgs = jvmArgs;
	}

	public String getVerify() {
		return verify;
	}

	@Option(option = "verify", description = "run in verify mode")
	public void setVerify(String verify) {
		this.verify = verify;
	}

	@TaskAction
	public void exec() {
		setMain("org.openjdk.jmh.Main");
		File resultFile = getProject().file("build/reports/" + getName() + "/result." + format);

		if (include != null) {
			args(".*" + include + ".*");
		}
		else if (fullInclude != null) {
			args(fullInclude);
		}

		if(exclude != null) {
			args("-e", exclude);
		}
		if(verify != null) { // execute benchmarks with the minimum amount of execution (only to check if they are working)
			System.out.println("Running in verify mode");
			args("-f", 1);
			args("-wi", 1);
			args("-i", 1);
		}
		args("-foe", "true"); //fail-on-error
		args("-v", "NORMAL"); //verbosity [SILENT, NORMAL, EXTRA]
		if(profilers != null) {
			for (String prof : profilers.split(",")) {
				args("-prof", prof);
			}
		}
		args("-jvmArgsPrepend", "-Xmx3072m");
		args("-jvmArgsPrepend", "-Xms3072m");
		if(jmhJvmArgs != null) {
			for(String jvmArg : jmhJvmArgs.split(" ")) {
				args("-jvmArgsPrepend", jvmArg);
			}
		}
		args("-rf", format);
		args("-rff", resultFile);

		System.out.println("\nExecuting JMH with: " + getArgs() + "\n");
		resultFile.getParentFile().mkdirs();

		super.exec();
	}
}
