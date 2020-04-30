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

import java.util.regex.Pattern;

import org.gradle.api.InvalidUserDataException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

/**
 * Looks for a {@code -PversionBranch=foo} type of property and uses that as an additional
 * suffix after the snapshot qualifier ({@code -SNAPSHOT}), in order to generate branch-specific
 * snapshots that can be used to eg. validate a PR.
 * <p>
 * The custom suffix must only contain alphanumeric characters, and is appended to the version
 * string, separated by a dash (eg. {@code 1.2.3-SNAPSHOT-foo}).
 *
 * @author Simon Baslé
 */
public class CustomVersionPlugin implements Plugin<Project> {

	private static final String CUSTOM_VERSION_PROPERTY = "versionBranch";
	private static final String SNAPSHOT_SUFFIX         = "-SNAPSHOT";

	private static final Pattern ONLY_ALPHANUMERIC_PATTERN = Pattern.compile("[A-Za-z0-9]+");

	@Override
	public void apply(Project project) {
		String version = project.getVersion().toString();
		Object versionBranchProperty = project.getProperties().get(CUSTOM_VERSION_PROPERTY);
		if (versionBranchProperty == null) {
			return;
		}

		String versionBranch = versionBranchProperty.toString();
		//consider an empty string as "no custom version"
		if (versionBranch.isEmpty()) {
			return;
		}
		if (!ONLY_ALPHANUMERIC_PATTERN.matcher(versionBranch).matches()) {
			throw new InvalidUserDataException("Custom version for project passed through -PversionBranch must be alphanumeric chars only: " + versionBranch);
		}

		if (!version.endsWith(SNAPSHOT_SUFFIX)) {
			return;
		}

		String realVersion = version + "-" + versionBranch;
		project.setVersion(realVersion);
		System.out.println("Building custom snapshot for " + project + ": '" + project.getVersion());
	}

}
