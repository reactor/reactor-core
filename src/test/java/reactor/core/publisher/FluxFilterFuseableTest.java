/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.mockito.Mockito;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.publisher.FluxOperatorTest;

public class FluxFilterFuseableTest extends FluxOperatorTest<String, String> {

    @Test
    public void scanSubscriber() {
        Subscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxFilterFuseable.FilterFuseableSubscriber<String> test = new FluxFilterFuseable.FilterFuseableSubscriber<>(actual, t -> true);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
    }

    @Test
    public void scanConditionalSubscriber() {
        Fuseable.ConditionalSubscriber<String> actual = Mockito.mock(Fuseable.ConditionalSubscriber.class);
        FluxFilterFuseable.FilterFuseableConditionalSubscriber<String> test = new FluxFilterFuseable.FilterFuseableConditionalSubscriber<>(actual, t -> true);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
    }
}
