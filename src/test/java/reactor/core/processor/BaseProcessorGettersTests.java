package reactor.core.processor;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Sergey Shcherbakov
 * @author Stephane Maldini
 */
public class BaseProcessorGettersTests {

	@Test
	public void testRingBufferProcessorGetters() {

		final int TEST_BUFFER_SIZE = 16;
		ExecutorProcessor<Object, Object> processor = RingBufferProcessor.create("testProcessor", TEST_BUFFER_SIZE);

		assertEquals(TEST_BUFFER_SIZE, processor.getAvailableCapacity());

		processor.awaitAndShutdown();

	}

	@Test
	public void testRingBufferWorkProcessorGetters() {

		final int TEST_BUFFER_SIZE = 16;
		ExecutorProcessor<Object, Object> processor = RingBufferWorkProcessor.create("testProcessor", TEST_BUFFER_SIZE);

		assertEquals(TEST_BUFFER_SIZE, processor.getAvailableCapacity());

		processor.onNext(new Object());

		assertEquals(TEST_BUFFER_SIZE - 1, processor.getAvailableCapacity());
		processor.awaitAndShutdown();

	}

}
