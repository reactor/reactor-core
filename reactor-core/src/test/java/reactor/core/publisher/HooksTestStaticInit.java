/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class HooksTestStaticInit {

	//IMPORTANT: this test case depends on System property
	// `reactor.trace.operatorStacktrace` be set to `true`

	@After
	public void resetAllHooks() {
		Hooks.resetOnOperatorError();
		Hooks.resetOnNextDropped();
		Hooks.resetOnErrorDropped();
//		Hooks.resetOnOperatorDebug(); //superseded by resetOnEachOperator
		Hooks.resetOnEachOperator();
		Hooks.resetOnLastOperator();
	}

	@Test
	public void syspropDebugModeShouldNotFail() {
		assertThat(System.getProperties())
				.as("debug mode set via system property")
				.containsEntry("reactor.trace.operatorStacktrace", "true");
		assertThat(Hooks.getOnEachOperatorHooks())
				.as("debug hook activated")
				.containsKey(Hooks.ON_OPERATOR_DEBUG_KEY);
		//would throw NPE due to https://github.com/reactor/reactor-core/issues/985
		Mono.just("hello").subscribe();
	}

}
