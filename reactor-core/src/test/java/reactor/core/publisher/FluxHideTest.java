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
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.publisher.TestPublisher;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxHideTest {

	@Test
	public void normal() {
		Flux<Integer> f = Flux.just(1);
		assertThat(f).isInstanceOf(Fuseable.ScalarCallable.class);
		f = f.hide();
		assertThat(f).isNotInstanceOf(Fuseable.ScalarCallable.class);
		assertThat(f).isInstanceOf(FluxHide.class);
	}

	@Test
	public void suppressedSubscriber() {
		CoreSubscriber<Integer> s = new CoreSubscriber<Integer>() {
			@Override
			public void onSubscribe(
					Subscription s) {
				s.request(Long.MAX_VALUE);
			}

			@Override
			public void onNext(
					Integer integer) {

			}

			@Override
			public void onError(
					Throwable t) {

			}

			@Override
			public void onComplete() {

			}
		};

		FluxHide.SuppressFuseableSubscriber<Integer> sfs = Flux.just(1, 2, 3)
		                                              .subscribeWith(new FluxHide.SuppressFuseableSubscriber<>(s));

		assertThat(sfs.actual()).isEqualTo(s);
		assertThat(sfs.size()).isEqualTo(0);
		assertThat(sfs.isEmpty()).isFalse();
		assertThat(sfs.poll()).isNull();
		assertThat(sfs.requestFusion(Fuseable.ANY)).isEqualTo(Fuseable.NONE);

		sfs.clear(); //NOOP

		TestPublisher<Integer> ts = TestPublisher.create();
		ts.subscribe(sfs);
		ts.assertCancelled();
	}

	@Test
	public void suppressedSubscriberError() {
		CoreSubscriber<Integer> s = new CoreSubscriber<Integer>() {
			@Override
			public void onSubscribe(
					Subscription s) {
				s.cancel();
			}

			@Override
			public void onNext(
					Integer integer) {

			}

			@Override
			public void onError(
					Throwable t) {

			}

			@Override
			public void onComplete() {

			}
		};

		FluxHide.SuppressFuseableSubscriber sfs = Flux.<Integer>error(new Exception("test"))
		                                              .subscribeWith(new FluxHide.SuppressFuseableSubscriber<>(s));

		assertThat(sfs.actual()).isEqualTo(s);
		assertThat(sfs.size()).isEqualTo(0);
		assertThat(sfs.isEmpty()).isFalse();
		assertThat(sfs.poll()).isNull();
		assertThat(sfs.requestFusion(Fuseable.ANY)).isEqualTo(Fuseable.NONE);

		sfs.clear(); //NOOP
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxHide<Integer> test = new FluxHide<>(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    public void scanSubscriber() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxHide.HideSubscriber<String> test = new FluxHide.HideSubscriber<>(actual);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }

	@Test
    public void scanSuppressFuseableSubscriber() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxHide.SuppressFuseableSubscriber<String> test = new FluxHide.SuppressFuseableSubscriber<>(actual);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }
}
