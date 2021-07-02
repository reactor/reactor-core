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
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxSourceMonoTest {

    @Test
    public void scanOperatorWithSyncSource(){
        Mono<String> source = Mono.just("Foo");
        FluxSourceMono<String> test = new FluxSourceMono<>(source);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(-1);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }

    @Test
    public void scanOperatorWithAsyncSource(){
        MonoDelayElement<String> source = new MonoDelayElement<>(Mono.empty(), 1, TimeUnit.SECONDS, Schedulers.immediate());

        FluxSourceMono<String> test = new FluxSourceMono<>(source);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(-1);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
    }

    @Test
    public void scanFuseableOperatorWithSyncSource(){
        Mono<String> source = Mono.just("Foo");
        FluxSourceMonoFuseable<String> test = new FluxSourceMonoFuseable<>(source);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(-1);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }

    @Test
    public void scanFuseableOperatorWithAsyncSource(){
        MonoDelayElement<String> source = new MonoDelayElement<>(Mono.empty(), 1, TimeUnit.SECONDS, Schedulers.immediate());

        FluxSourceMonoFuseable<String> test = new FluxSourceMonoFuseable<>(source);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(-1);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
    }

}