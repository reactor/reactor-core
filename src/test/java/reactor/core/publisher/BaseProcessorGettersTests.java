package reactor.core.publisher;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Sergey Shcherbakov
 * @author Stephane Maldini
 */
public class BaseProcessorGettersTests {

	@Test
	public void testTopicProcessorGetters() {

		final int TEST_BUFFER_SIZE = 16;
		ExecutorProcessor<Object, Object> processor = TopicProcessor.create("testProcessor", TEST_BUFFER_SIZE);

		assertEquals(TEST_BUFFER_SIZE, processor.getAvailableCapacity());

		processor.awaitAndShutdown();

	}

	@Test
	public void testWorkQueueProcessorGetters() {

		final int TEST_BUFFER_SIZE = 16;
		ExecutorProcessor<Object, Object> processor = WorkQueueProcessor.create("testProcessor", TEST_BUFFER_SIZE);

		assertEquals(TEST_BUFFER_SIZE, processor.getAvailableCapacity());

		processor.onNext(new Object());

		assertEquals(TEST_BUFFER_SIZE - 1, processor.getAvailableCapacity());
		processor.awaitAndShutdown();

	}

}
