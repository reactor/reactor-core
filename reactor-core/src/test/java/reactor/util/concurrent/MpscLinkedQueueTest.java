package reactor.util.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MpscLinkedQueueTest {

	@Test
	public void mpscQueuesAPI() {
		assertThat(Queues.unboundedMultiproducer().get()).isInstanceOf(MpscLinkedQueue.class);
	}

	@Test
	public void shouldRejectNullableValues() {
		MpscLinkedQueue<Object> q = new MpscLinkedQueue<>();
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			q.offer(null);
		});
	}

	@Test
	public void shouldRejectNullableValuesForTest() {
		MpscLinkedQueue<Object> q = new MpscLinkedQueue<>();
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			q.test(null, null);
		});
	}

	@Test
	public void shouldNormallyOfferTwoValues() {
		MpscLinkedQueue<Object> q = new MpscLinkedQueue<Object>();
		q.test(1, 2);

		assertThat(q.poll()).isEqualTo(1);
		assertThat(q.poll()).isEqualTo(2);
		assertThat(q.poll()).isNull();
	}

	@Test
	public void shouldNotAllowIteratingWithIterator() {
		MpscLinkedQueue<Object> q = new MpscLinkedQueue<>();

		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> {
			q.iterator();
		});
	}

	@Test
	public void shouldNotAllowElementsRemoving() {
		MpscLinkedQueue<Object> q = new MpscLinkedQueue<>();

		q.offer(1);
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> {
			q.remove(1);
		});
	}

	@Test
	public void shouldClearQueue() {
		MpscLinkedQueue<Object> q = new MpscLinkedQueue<>();
		q.test(1, 2);

		assertThat(q.isEmpty()).as("isEmpty() false").isFalse();
		assertThat(q).hasSize(2);

		q.clear();

		assertThat(q.isEmpty()).as("isEmpty() true").isTrue();
		assertThat(q).hasSize(0);
	}

	@Test
	public void shouldNotRemoveElementOnPeek() {
		MpscLinkedQueue<Object> q = new MpscLinkedQueue<>();
		q.test(1, 2);

		for (int i = 0; i < 100; i++) {
			assertThat(q.peek()).isEqualTo(1);
			assertThat(q).hasSize(2);
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
