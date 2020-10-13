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

package reactor.guide;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;

import static org.assertj.core.api.Assertions.assertThat;

//this test is here to have a runnable demonstration of a more advanced traceback in
// debugging mode. it was put outside of reactor.core package so that the traceback shows
// more details
public class GuideDebuggingExtraTests {

	@Test
	public void debuggingActivatedWithDeepTraceback() {
		Hooks.onOperatorDebug();

		StringWriter sw = new StringWriter();
		FakeRepository.findAllUserByName(Flux.just("pedro", "simon", "stephane"))
		              .transform(FakeUtils1.applyFilters)
		              .transform(FakeUtils2.enrichUser)
		              .subscribe(System.out::println,
				              t -> t.printStackTrace(new PrintWriter(sw))
		              );

		String debugStack = sw.toString();

		assertThat(debugStack.substring(0, debugStack.indexOf("Stack trace:")))
				.endsWith("Error has been observed at the following site(s):\n"
						+ "\t|_       Flux.map ⇢ at reactor.guide.FakeRepository.findAllUserByName(FakeRepository.java:27)\n"
						+ "\t|_       Flux.map ⇢ at reactor.guide.FakeRepository.findAllUserByName(FakeRepository.java:28)\n"
						+ "\t|_    Flux.filter ⇢ at reactor.guide.FakeUtils1.lambda$static$1(FakeUtils1.java:29)\n"
						+ "\t|_ Flux.transform ⇢ at reactor.guide.GuideDebuggingExtraTests.debuggingActivatedWithDeepTraceback(GuideDebuggingExtraTests.java:39)\n"
						+ "\t|_   Flux.elapsed ⇢ at reactor.guide.FakeUtils2.lambda$static$0(FakeUtils2.java:30)\n"
						+ "\t|_ Flux.transform ⇢ at reactor.guide.GuideDebuggingExtraTests.debuggingActivatedWithDeepTraceback(GuideDebuggingExtraTests.java:40)\n");
	}
}
