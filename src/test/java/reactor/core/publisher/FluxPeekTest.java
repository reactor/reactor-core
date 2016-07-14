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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.subscriber.SubscriptionHelper;
import reactor.util.concurrent.QueueSupplier;
import reactor.test.subscriber.TestSubscriber;
import reactor.util.Exceptions;

public class FluxPeekTest {
	@Test(expected = NullPointerException.class)
	public void nullSource() {
		new FluxPeek<>(null, null, null, null, null, null, null, null);
	}

	@Test
	public void normal() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		AtomicLong onRequest = new AtomicLong();
		AtomicBoolean onAfterComplete = new AtomicBoolean();
		AtomicBoolean onCancel = new AtomicBoolean();

		new FluxPeek<>(new FluxJust<>(1),
		  onSubscribe::set,
		  onNext::set,
		  onError::set,
		  () -> onComplete.set(true),
		  () -> onAfterComplete.set(true),
		  onRequest::set,
		  () -> onCancel.set(true)
		).subscribe(ts);

		Assert.assertNotNull(onSubscribe.get());
		Assert.assertEquals((Integer) 1, onNext.get());
		Assert.assertNull(onError.get());
		Assert.assertTrue(onComplete.get());
		Assert.assertTrue(onAfterComplete.get());
		Assert.assertEquals(Long.MAX_VALUE, onRequest.get());
		Assert.assertFalse(onCancel.get());
	}

	@Test
	public void error() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		AtomicLong onRequest = new AtomicLong();
		AtomicBoolean onAfterComplete = new AtomicBoolean();
		AtomicBoolean onCancel = new AtomicBoolean();

		new FluxPeek<>(new MonoError<>(new RuntimeException("forced failure")),
		  onSubscribe::set,
		  onNext::set,
		  onError::set,
		  () -> onComplete.set(true),
		  () -> onAfterComplete.set(true),
		  onRequest::set,
		  () -> onCancel.set(true)
		).subscribe(ts);

		Assert.assertNotNull(onSubscribe.get());
		Assert.assertNull(onNext.get());
		Assert.assertTrue(onError.get() instanceof RuntimeException);
		Assert.assertFalse(onComplete.get());
		Assert.assertTrue(onAfterComplete.get());
		Assert.assertEquals(Long.MAX_VALUE, onRequest.get());
		Assert.assertFalse(onCancel.get());
	}

	@Test
	public void empty() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		AtomicLong onRequest = new AtomicLong();
		AtomicBoolean onAfterComplete = new AtomicBoolean();
		AtomicBoolean onCancel = new AtomicBoolean();

		new FluxPeek<>(MonoEmpty.instance(),
		  onSubscribe::set,
		  onNext::set,
		  onError::set,
		  () -> onComplete.set(true),
		  () -> onAfterComplete.set(true),
		  onRequest::set,
		  () -> onCancel.set(true)
		).subscribe(ts);

		Assert.assertNotNull(onSubscribe.get());
		Assert.assertNull(onNext.get());
		Assert.assertNull(onError.get());
		Assert.assertTrue(onComplete.get());
		Assert.assertTrue(onAfterComplete.get());
		Assert.assertEquals(Long.MAX_VALUE, onRequest.get());
		Assert.assertFalse(onCancel.get());
	}

	@Test
	public void never() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		AtomicLong onRequest = new AtomicLong();
		AtomicBoolean onAfterComplete = new AtomicBoolean();
		AtomicBoolean onCancel = new AtomicBoolean();

		new FluxPeek<>(FluxNever.instance(),
		  onSubscribe::set,
		  onNext::set,
		  onError::set,
		  () -> onComplete.set(true),
		  () -> onAfterComplete.set(true),
		  onRequest::set,
		  () -> onCancel.set(true)
		).subscribe(ts);

		Assert.assertNotNull(onSubscribe.get());
		Assert.assertNull(onNext.get());
		Assert.assertNull(onError.get());
		Assert.assertFalse(onComplete.get());
		Assert.assertFalse(onAfterComplete.get());
		Assert.assertEquals(Long.MAX_VALUE, onRequest.get());
		Assert.assertFalse(onCancel.get());
	}

	@Test
	public void neverCancel() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		AtomicLong onRequest = new AtomicLong();
		AtomicBoolean onAfterComplete = new AtomicBoolean();
		AtomicBoolean onCancel = new AtomicBoolean();

		new FluxPeek<>(FluxNever.instance(),
		  onSubscribe::set,
		  onNext::set,
		  onError::set,
		  () -> onComplete.set(true),
		  () -> onAfterComplete.set(true),
		  onRequest::set,
		  () -> onCancel.set(true)
		).subscribe(ts);

		Assert.assertNotNull(onSubscribe.get());
		Assert.assertNull(onNext.get());
		Assert.assertNull(onError.get());
		Assert.assertFalse(onComplete.get());
		Assert.assertFalse(onAfterComplete.get());
		Assert.assertEquals(Long.MAX_VALUE, onRequest.get());
		Assert.assertFalse(onCancel.get());

		ts.cancel();

		Assert.assertTrue(onCancel.get());
	}

	@Test
	public void callbackError(){
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Throwable err = new Exception("test");

		Flux.just(1).doOnNext(d -> {throw Exceptions.propagate(err);}).subscribe(ts);

		//nominal error path (DownstreamException)
		ts.assertErrorMessage("test");

		ts = TestSubscriber.create();

		try {
			Flux.just(1).doOnNext(d -> {throw Exceptions.bubble(err);}).subscribe(ts);

			Assert.fail();
		}
		catch (Exception e){
			//fatal publisher exception (BubblingException)
			Assert.assertTrue(Exceptions.unwrap(e) == err);
		}
	}

	@Test
	public void syncFusionAvailable() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux.range(1, 2)
		    .doOnNext(v -> {
		    })
		    .subscribe(ts);

		Subscription s = ts.upstream();
		Assert.assertTrue("Non-fuseable upstream: " + s,
				s instanceof Fuseable.QueueSubscription);
	}

	@Test
	public void asyncFusionAvailable() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		new UnicastProcessor<>(QueueSupplier.<Integer>get(2).get()).doOnNext(v -> {
		})
		                                                                  .subscribe(ts);

		Subscription s = ts.upstream();
		Assert.assertTrue("Non-fuseable upstream" + s,
				s instanceof Fuseable.QueueSubscription);
	}

	@Test
	public void conditionalFusionAvailable() {
		TestSubscriber<Object> ts = TestSubscriber.create();

		FluxSource.wrap(u -> {
			if (!(u instanceof Fuseable.ConditionalSubscriber)) {
				SubscriptionHelper.error(u,
						new IllegalArgumentException("The subscriber is not conditional: " + u));
			}
			else {
				SubscriptionHelper.complete(u);
			}
		})
		    .doOnNext(v -> {
		    })
		    .filter(v -> true)
		    .subscribe(ts);

		ts.assertNoError()
		  .assertNoValues()
		  .assertComplete();
	}

	@Test
	public void conditionalFusionAvailableWithFuseable() {
		TestSubscriber<Object> ts = TestSubscriber.create();

		FluxSource.wrap(u -> {
			if (!(u instanceof Fuseable.ConditionalSubscriber)) {
				SubscriptionHelper.error(u,
						new IllegalArgumentException("The subscriber is not conditional: " + u));
			}
			else {
				SubscriptionHelper.complete(u);
			}
		})
		    .doOnNext(v -> {
		    })
		    .filter(v -> true)
		    .subscribe(ts);

		ts.assertNoError()
		  .assertNoValues()
		  .assertComplete();
	}

	@Test
	public void syncCompleteCalled() {
		AtomicBoolean onComplete = new AtomicBoolean();

		TestSubscriber<Object> ts = TestSubscriber.create();

		Flux.range(1, 2)
		    .doOnComplete(() -> onComplete.set(true))
		    .subscribe(ts);

		ts.assertNoError()
		  .assertValues(1, 2)
		  .assertComplete();

		Assert.assertTrue("onComplete not called back", onComplete.get());
	}

	@Test
	public void syncdoAfterTerminateCalled() {
		AtomicBoolean onTerminate = new AtomicBoolean();

		TestSubscriber<Object> ts = TestSubscriber.create();

		Flux.range(1, 2)
		    .doAfterTerminate(() -> onTerminate.set(true))
		    .subscribe(ts);

		ts.assertNoError()
		  .assertValues(1, 2)
		  .assertComplete();

		Assert.assertTrue("onComplete not called back", onTerminate.get());
	}

}
