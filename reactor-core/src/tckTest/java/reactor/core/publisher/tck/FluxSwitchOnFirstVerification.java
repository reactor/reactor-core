/*
 * Copyright (c) 2018-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.publisher.tck;

import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;

@Test
public class FluxSwitchOnFirstVerification extends PublisherVerification<Integer> {

    public FluxSwitchOnFirstVerification() {
        super(new TestEnvironment());
    }

    @Override
    public Publisher<Integer> createPublisher(long elements) {
        return Flux.range(0, Integer.MAX_VALUE < elements ? Integer.MAX_VALUE : (int) elements)
                   .switchOnFirst((first, innerFlux) -> innerFlux);
    }

    @Override
    public Publisher<Integer> createFailedPublisher() {
        return Flux.<Integer>error(new RuntimeException())
                   .switchOnFirst((first, innerFlux) -> innerFlux);
    }

}