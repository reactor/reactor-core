package reactor.util.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import static org.junit.Assert.*;

public class MpscLinkedQueueTest {

	@Test(expected = NullPointerException.class)
	public void shouldRejectNullableValues() {
		MpscLinkedQueue<Object> q = new MpscLinkedQueue<>();
		q.offer(null);
	}

	@Test(expected = NullPointerException.class)
	public void shouldRejectNullableValuesForTest() {
		MpscLinkedQueue<Object> q = new MpscLinkedQueue<>();
		q.test(null, null);
	}

	@Test
	public void shouldNormallyOffersTwoValues() {
		MpscLinkedQueue<Object> q = new MpscLinkedQueue<Object>();
		q.test(1, 2);

		assertEquals(1, q.poll());
		assertEquals(2, q.poll());
		assertNull(q.poll());
	}

	@Test(expected = UnsupportedOperationException.class)
	public void shouldNotAllowIteratingWithIterator() {
		MpscLinkedQueue<Object> q = new MpscLinkedQueue<>();

		q.iterator();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void shouldNotAllowElementsRemoving() {
		MpscLinkedQueue<Object> q = new MpscLinkedQueue<>();

		q.offer(1);
		q.remove(1);
	}

	@Test
	public void shouldClearQueue() {
		MpscLinkedQueue<Object> q = new MpscLinkedQueue<>();
		q.test(1, 2);

		assertFalse(q.isEmpty());
		assertEquals(2, q.size());

		q.clear();

		assertTrue(q.isEmpty());
		assertEquals(0, q.size());
	}

	@Test
	public void shouldNotRemoveElementOnPeek() {
		MpscLinkedQueue<Object> q = new MpscLinkedQueue<>();
		q.test(1, 2);

		for (int i = 0; i < 100; i++) {
			assertEquals(1, q.peek());
			assertEquals(2, q.size());
		}
	}

	@Test
	public void mpscOfferPollRace() throws Exception {
		final MpscLinkedQueue<Integer> q = new MpscLinkedQueue<Integer>();

		final AtomicInteger c = new AtomicInteger(3);

		Thread t1 = new Thread(new Runnable() {
			int i;

			@Override
			public void run() {
				c.decrementAndGet();
				while (c.get() != 0) {
				}

				while (i++ < 10000) {
					q.offer(i);
				}
			}
		});
		t1.start();

		Thread t2 = new Thread(new Runnable() {
			int i = 10000;

			@Override
			public void run() {
				c.decrementAndGet();
				while (c.get() != 0) {
				}

				while (i++ < 10000) {
					q.offer(i);
				}
			}
		});
		t2.start();

		Runnable r3 = new Runnable() {
			int i = 20000;

			@Override
			public void run() {
				c.decrementAndGet();
				while (c.get() != 0) {
				}

				while (--i > 0) {
					q.poll();
				}
			}
		};

		r3.run();

		t1.join();
		t2.join();
	}

}
