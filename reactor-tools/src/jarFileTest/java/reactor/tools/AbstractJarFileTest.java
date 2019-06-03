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

import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.util.Collections.emptyMap;

/**
 * A helper class to access the content of a shaded JAR
 */
class AbstractJarFileTest {

	static Path root;

	static {
		try {
			Path jarFilePath = Paths.get(System.getProperty("jarFile"));
			URI jarFileUri = new URI("jar", jarFilePath.toUri().toString(), null);
			FileSystem fileSystem = FileSystems.newFileSystem(jarFileUri, emptyMap());
			root = fileSystem.getPath("/");
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
