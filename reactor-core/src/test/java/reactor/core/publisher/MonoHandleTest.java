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

import org.junit.Test;
import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;

public class MonoHandleTest {

	@Test
	public void normal() {
		Mono.just(1)
		    .handle((v, s) -> s.next(v * 2))
		    .subscribeWith(AssertSubscriber.create())
		    .assertContainValues(singleton(2))
		    .assertNoError()
		    .assertComplete();
	}
	@Test
	public void normalHide() {
		Mono.just(1)
		    .hide()
		    .handle((v, s) -> s.next(v * 2))
		    .subscribeWith(AssertSubscriber.create())
		    .assertContainValues(singleton(2))
		    .assertNoError()
		    .assertComplete();
	}

	@Test
	public void filterNullMapResult() {
		Mono.just(1)
		    .handle((v, s) -> { /*ignore*/ })
		    .subscribeWith(AssertSubscriber.create())
		    .assertValueCount(0)
		    .assertNoError()
		    .assertComplete();
	}

	@Test
	public void scanOperator(){
	    MonoHandle<Integer, Integer> test = new MonoHandle(Mono.just(1), (v, s) -> {});

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanFuseableOperator(){
		MonoHandleFuseable<Integer, Integer> test = new MonoHandleFuseable(Mono.just(1), (v, s) -> {});

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
