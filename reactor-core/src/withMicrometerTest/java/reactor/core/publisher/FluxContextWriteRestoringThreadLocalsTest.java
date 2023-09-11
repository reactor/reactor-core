/*
 * Copyright (c) 2020-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.publisher;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxContextWriteRestoringThreadLocalsTest {

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxContextWriteRestoringThreadLocals<Integer> test =
				new FluxContextWriteRestoringThreadLocals<>(parent, c -> c);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriber(){
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxContextWriteRestoringThreadLocals.ContextWriteRestoringThreadLocalsSubscriber<Integer>
				test =
				new FluxContextWriteRestoringThreadLocals.ContextWriteRestoringThreadLocalsSubscriber<>(
								actual, Context.empty()
				);

		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

}