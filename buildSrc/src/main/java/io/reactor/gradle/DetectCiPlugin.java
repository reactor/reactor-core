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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import javax.inject.Inject;

import org.gradle.api.Plugin;
import org.gradle.api.Project;

/**
 * @author Simon Baslé
 */
public class DetectCiPlugin implements Plugin<Project> {

	public static final List<String> CI_SERVERS = Arrays.asList("CI", "CONTINUOUS_INTEGRATION", "TRAVIS", "CIRCLECI", "bamboo_planKey", "GITHUB_ACTION");
	public static final String FLAG_EXTENSION = "isCiServer";
	public static final String DETECTED_EXTENSION = "detectedCiServers";

	public final Supplier<? extends Collection<String>> environmentSupplier;

	@Inject
	public DetectCiPlugin() {
		environmentSupplier = () -> System.getenv().keySet();
	}

	protected DetectCiPlugin(Collection<String> testEnvironment) {
		environmentSupplier = () -> testEnvironment;
	}

	@Override
	public void apply(Project project) {
		//ignore subprojects
		if (project.getParent() != null) {
			return;
		}

		Set<String> detected = new HashSet<>(CI_SERVERS);
		detected.retainAll(environmentSupplier.get());

		boolean isCiServer = !detected.isEmpty();
		if (isCiServer) {
			System.out.println("Detected CI environment: " + detected);
		}

		project.getExtensions().add(FLAG_EXTENSION, isCiServer);
		project.getExtensions().add(DETECTED_EXTENSION, detected);
	}
}
