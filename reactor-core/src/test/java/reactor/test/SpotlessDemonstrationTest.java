/*
 * Copyright (c) 2011-2021 VMware Inc. or its affiliates, All Rights Reserved.
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
package reactor.test;


import org.junit.jupiter.api.Test;

import reactor.core.publisher.Mono;


/**
 * @author Simon Baslé
 */
class SpotlessDemonstrationTest {

	/*
	 * There are several whitespace issues with this class:
	 *  - tests are separated with variations of blank lines that contains whitespace
	 *  - some indentation has spaces in multiples of 4, which should be replaced with tabs
	 *  - there are unused imports
	 *  - the file doesn't have a license header and doesn't end with a blank line
	 */


	@Test
	void withTooMuchAlignmentSpaces() {
		Mono.defer(() -> Mono.just(1)
							 .doOnTerminate(() -> { } ))
			.subscribe();
	}

	@Test
	void withASingleExtraAlignmentSpaces() {
		Mono.defer(() -> Mono.just(1)
							 .doOnTerminate(() -> { } ))
			.subscribe();
	}

	@Test
	void keepsSpacesAsIsAfterActualStartOfLine() {
		Mono.defer(() -> Mono.just(1)
							 .doOnTerminate(() -> {    } ))
			.subscribe();
		final String      var1 = "";
		final String variable2 = "";
	}
}
