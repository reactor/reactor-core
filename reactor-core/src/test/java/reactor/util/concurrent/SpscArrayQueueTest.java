package reactor.util.concurrent;

import java.util.Arrays;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SpscArrayQueueTest {

	@Test
	public void spscArrayQueuesAPI() {
		assertThat(Queues.xs().get()).isInstanceOf(SpscArrayQueue.class);
	}

	@Test(expected = NullPointerException.class)
	public void shouldRejectNullableValues() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);
		q.offer(null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void shouldNotAllowIteratingWithIterator() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);

		q.iterator();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void shouldNotAllowElementsRemoving() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);

		q.offer(1);
		q.remove(1);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void shouldNotAllowAllElementsRemoving() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);

		q.offer(1);
		q.offer(2);
		q.removeAll(Arrays.asList(1,2));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void shouldNotAllowAllElementsRetaining() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);

		q.offer(1);
		q.offer(2);
		q.retainAll(Arrays.asList(1,2));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void shouldNotAllowAdd() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);
		q.add(1);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void shouldNotAllowAddAll() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);
		q.addAll(Arrays.asList(1,2,3));
	}

	@Test
	public void shouldClearQueue() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);
		q.offer(1);
		q.offer(2);

		assertThat(q.isEmpty()).as("isEmpty() false").isFalse();
		assertThat(q.size()).isEqualTo(2);

		q.clear();

		assertThat(q.isEmpty()).as("isEmpty() true").isTrue();
		assertThat(q.size()).isEqualTo(0);
	}

	@Test
	public void shouldNotRemoveElementOnPeek() {
		SpscArrayQueue<Object> q = new SpscArrayQueue<>(32);
		q.offer(1);
		q.offer(2);

		for (int i = 0; i < 100; i++) {
			assertThat(q.peek()).isEqualTo(1);
			assertThat(q.size()).isEqualTo(2);		}
	}
}
