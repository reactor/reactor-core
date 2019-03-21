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
package reactor.core.publisher.scenarios;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.TopicProcessor;
import reactor.core.publisher.WorkQueueProcessor;
import reactor.core.scheduler.Schedulers;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephane Maldini
 */
public class ConsistentProcessorTests {
	private Processor<String, String> processor;
	private Processor<String, String> workProcessor;

	@Test
	@Ignore
	public void testMultipleConsumersMultipleTimes() throws Exception {
		Sender sender = new Sender();

		int count = 1000;
		int threads = 6;
		int fulltotaltext = 0;
		int fulltotalints = 0;
		int iter = 10;

		List<Integer> total = new ArrayList<>();
		for (int t = 0; t < iter; t++) {
			List<List<String>> clientDatas = getClientDatas(threads, sender, count);

			assertThat(clientDatas.size(), is(threads));

			List<String> numbersNoEnds = new ArrayList<>();
			List<Integer> numbersNoEndsInt = new ArrayList<>();
			for (int i = 0; i < clientDatas.size(); i++) {
				List<String> datas = clientDatas.get(i);
				assertThat(datas, notNullValue());
				StringBuffer buf = new StringBuffer();
				for (int j = 0; j < datas.size(); j++) {
					buf.append(datas.get(j));
				}

				List<String> split = split(buf.toString());
				for (int x = 0; x < split.size(); x++) {
					String d = split.get(x);
					if (d != null && !d.trim().isEmpty() && !d.contains("END")) {
						fulltotaltext += 1;
						numbersNoEnds.add(d);
						int intnum = Integer.parseInt(d);
						total.add(intnum);
						if (!numbersNoEndsInt.contains(intnum)) {
							numbersNoEndsInt.add(intnum);
							fulltotalints += 1;
						}
					}
				}
			}

			String msg = "Run number " + t;
			Collections.sort(numbersNoEndsInt);
			System.out.println(msg + " with received :" + numbersNoEndsInt.size());
			System.out.println("dups:" + findDuplicates(numbersNoEndsInt));
			// we can't measure individual session anymore so just
			// check that below lists match.
			assertThat(msg, numbersNoEndsInt.size(), is(numbersNoEnds.size()));
		}
		Set<Integer> dups = findDuplicates(total);
		System.out.println("total dups:" + dups);
		System.out.println("total int:" + fulltotalints);
		System.out.println("total text:" + fulltotaltext);
		// check full totals because we know what this should be
		assertThat(fulltotalints, is(count * iter));
		assertThat(fulltotaltext, is(count * iter));
		assertTrue(dups.isEmpty());
	}

	@Before
	public void loadEnv() {
		setupPipeline();
	}

	@After
	public void clean() throws Exception {
	}

	@Test
	public void noop(){

	}

	public Set<Integer> findDuplicates(List<Integer> listContainingDuplicates) {
		final Set<Integer> setToReturn = new HashSet<>();
		final Set<Integer> set1 = new HashSet<>();

		for (Integer yourInt : listContainingDuplicates) {
			if (!set1.add(yourInt)) {
				setToReturn.add(yourInt);
			}
		}
		return setToReturn;
	}

	private void setupPipeline() {
		processor = TopicProcessor.<String>builder().autoCancel(false).build();
		workProcessor = WorkQueueProcessor.<String>builder().autoCancel(false).build();
		processor.subscribe(workProcessor);
	}

	private Receiver getClientDataPromise() throws Exception {
		Receiver r = new Receiver();
		workProcessor.subscribe(r);
		return r;
	}

	private List<List<String>> getClientDatas(int threadCount, final Sender sender, int count) throws Exception {
		final CountDownLatch promiseLatch = new CountDownLatch(threadCount);

		final ArrayList<List<String>> datas = new ArrayList<>();


		Runnable srunner = new Runnable() {
		    @Override
			public void run() {
				try {
					sender.sendNext(count);
				} catch (Exception ie) {
					ie.printStackTrace();
				}
			}
		};
		Thread st = new Thread(srunner, "SenderThread");

		st.start();

		final Random r = new Random();

		for (int i = 0; i < threadCount; ++i) {
			Runnable runner = new Runnable() {
			    @Override
				public void run() {
					try {
						Thread.sleep(r.nextInt(2000) + 500);
						Receiver clientDataPromise = getClientDataPromise();
						clientDataPromise.latch.await(20, TimeUnit.SECONDS);
						datas.add(clientDataPromise.data);
						promiseLatch.countDown();
					} catch (Exception ie) {
						ie.printStackTrace();
					}
				}
			};
			Thread t = new Thread(runner, "SmokeThread" + i);

			t.start();
		}
		promiseLatch.await();


		return datas;
	}

	private static List<String> split(String data) {
		return Arrays.asList(data.split("\\r?\\n"));
	}

	class Sender {
		int x = 0;

		void sendNext(int count) {
			for (int i = 0; i < count; i++) {
//				System.out.println("XXXX " + x);
				processor.onNext((x++) + "\n");
			}
		}
	}

	static int subCount = 0;

	class Receiver implements CoreSubscriber<String> {

		final int            id    = ++subCount;
		final List<String>   data  = new ArrayList<>();
		final CountDownLatch latch = new CountDownLatch(1);

		@Override
		public void onSubscribe(Subscription s) {
			Schedulers.single().schedule(() -> finish(s), 5000, TimeUnit.MILLISECONDS);
			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(String s) {
			data.add(s);
		}

		@Override
		public void onError(Throwable t) {
			t.printStackTrace();
		}

		@Override
		public void onComplete() {
			finish(null);
		}

		void finish(Subscription s) {
			if (s != null) {
				s.cancel();
			}
			latch.countDown();
			System.out.println("Receiver " + id + " completed");
		}
	}
}
