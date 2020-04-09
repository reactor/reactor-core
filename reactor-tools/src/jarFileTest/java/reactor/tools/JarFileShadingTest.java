/*
 * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.tools;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test must be executed with Gradle because it requires a shadow JAR
 */
public class JarFileShadingTest extends AbstractJarFileTest {

	@Test
	public void testPackages() throws Exception {
		assertThatFileList(root).containsOnly(
				"reactor",
				"META-INF"
		);

		assertThatFileList(root.resolve("reactor")).containsOnly(
				"tools"
		);
	}

	@Test
	public void testMetaInf() throws Exception {
		assertThatFileList(root.resolve("META-INF")).containsOnly(
				"MANIFEST.MF"
		);
	}

	private ListAssert<String> assertThatFileList(Path path) throws IOException {
		return (ListAssert) assertThat(Files.list(path))
				.extracting(Path::getFileName)
				.extracting(Path::toString)
				.extracting(it -> it.endsWith("/") ? it.substring(0, it.length() - 1) : it);
	}

}
