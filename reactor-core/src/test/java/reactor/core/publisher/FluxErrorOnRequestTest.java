/*
 * Copyright (c) 2020-2021 VMware Inc. or its affiliates, All Rights Reserved.
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
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxErrorOnRequestTest {

	@Test
	public void scanOperator(){
	    FluxErrorOnRequest test = new FluxErrorOnRequest(new IllegalStateException());

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	    assertThat(test.scan(Scannable.Attr.ACTUAL)).isNull();
	}

	@Test
	public void scanSubscription() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, sub -> sub.request(100));
		IllegalStateException error = new IllegalStateException();
		FluxErrorOnRequest.ErrorSubscription test = new FluxErrorOnRequest.ErrorSubscription(actual, error);

		assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(error);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}