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

import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxErrorTest {

	@Test
	public void normal() {
		StepVerifier.create(Flux.error(new Exception("test")))
		            .verifyErrorMessage("test");
	}

    @Test
    public void scanSubscription() {
	    @SuppressWarnings("unchecked") CoreSubscriber<String> subscriber = Mockito.mock(CoreSubscriber.class);
        FluxError.ErrorSubscription test =
                new FluxError.ErrorSubscription(subscriber, new IllegalStateException("boom"));

        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(subscriber);
        assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).hasMessage("boom");
        test.request(1);
        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
    }

    @Test
    public void scanSubscriptionCancelled() {
	    @SuppressWarnings("unchecked")
	    CoreSubscriber<String> subscriber = Mockito.mock(CoreSubscriber.class);
        FluxError.ErrorSubscription test =
                new FluxError.ErrorSubscription(subscriber, new IllegalStateException("boom"));

        assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }
}