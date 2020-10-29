/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.publisher;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThat;

public class MonoErrorTest {

	@Test
	public void normal() {
		StepVerifier.create(Mono.error(new Exception("test")))
		            .verifyErrorMessage("test");
	}

	@Test
	public void onMonoRejectedThrowOnBlock() {
		assertThatExceptionOfType(Exception.class).isThrownBy(() -> {
			Mono.error(new Exception("test"))
					.block();
		});
	}

	@Test
	public void scanOperator(){
		MonoError test = new MonoError(new NullPointerException());

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
