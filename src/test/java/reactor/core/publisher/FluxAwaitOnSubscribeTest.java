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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxAwaitOnSubscribeTest {

	@Test
	public void requestDelayed() {
		AtomicBoolean state = new AtomicBoolean();
		AtomicReference<Throwable> e = new AtomicReference<>();

		Flux.just(1)
		    .awaitOnSubscribe()
		    .subscribe(new Subscriber<Integer>() {
			    boolean open;

			    @Override
			    public void onSubscribe(Subscription s) {
				    s.request(1);
				    open = true;
			    }

			    @Override
			    public void onNext(Integer t) {
				    state.set(open);
			    }

			    @Override
			    public void onError(Throwable t) {
				    e.set(t);
			    }

			    @Override
			    public void onComplete() {

			    }
		    });

		Assert.assertNull("Error: " + e.get(), e.get());

		Assert.assertTrue("Not open!", state.get());
	}

	@Test
	public void cancelDelayed() {
		AtomicBoolean state1 = new AtomicBoolean();
		AtomicBoolean state2 = new AtomicBoolean();
		AtomicReference<Throwable> e = new AtomicReference<>();

		DirectProcessor<Integer> sp = DirectProcessor.create();

		sp.awaitOnSubscribe()
		  .doOnCancel(() -> state2.set(state1.get()))
		  .subscribe(new Subscriber<Integer>() {
			  @Override
			  public void onSubscribe(Subscription s) {
				  s.cancel();
				  state1.set(true);
			  }

			  @Override
			  public void onNext(Integer t) {
			  }

			  @Override
			  public void onError(Throwable t) {
				  e.set(t);
			  }

			  @Override
			  public void onComplete() {

			  }
		  });

		Assert.assertNull("Error: " + e.get(), e.get());

		Assert.assertFalse("Cancel executed before onSubscribe finished", state2.get());
		Assert.assertFalse("Has subscribers?!", sp.hasDownstreams());
	}

	@Test
	public void requestNotDelayed() {
		AtomicBoolean state = new AtomicBoolean();
		AtomicReference<Throwable> e = new AtomicReference<>();

		Flux.just(1)
		    .subscribe(new Subscriber<Integer>() {
			    boolean open;

			    @Override
			    public void onSubscribe(Subscription s) {
				    s.request(1);
				    open = true;
			    }

			    @Override
			    public void onNext(Integer t) {
				    state.set(open);
			    }

			    @Override
			    public void onError(Throwable t) {
				    e.set(t);
			    }

			    @Override
			    public void onComplete() {

			    }
		    });

		Assert.assertNull("Error: " + e.get(), e.get());

		Assert.assertFalse("Request delayed!", state.get());
	}

	@Test
	public void cancelNotDelayed() {
		AtomicBoolean state1 = new AtomicBoolean();
		AtomicBoolean state2 = new AtomicBoolean();
		AtomicReference<Throwable> e = new AtomicReference<>();

		Flux.just(1)
		    .doOnCancel(() -> state2.set(state1.get()))
		    .subscribe(new Subscriber<Integer>() {
			    @Override
			    public void onSubscribe(Subscription s) {
				    s.cancel();
				    state1.set(true);
			    }

			    @Override
			    public void onNext(Integer t) {
			    }

			    @Override
			    public void onError(Throwable t) {
				    e.set(t);
			    }

			    @Override
			    public void onComplete() {

			    }
		    });

		Assert.assertNull("Error: " + e.get(), e.get());

		Assert.assertFalse("Cancel executed before onSubscribe finished", state2.get());
	}

	@Test
	public void scanPostOnSubscribeSubscriber() {
		Subscriber<String> s = new LambdaSubscriber<>(null, null, null, null);
		FluxAwaitOnSubscribe.PostOnSubscribeSubscriber<String> test = new FluxAwaitOnSubscribe.PostOnSubscribeSubscriber<>(s);

		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(s);
		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isNull();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(0L);

		test.request(2);
		assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(2L);

		Subscription parent = Operators.cancelledSubscription();
		test.onSubscribe(parent);
		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}

}
