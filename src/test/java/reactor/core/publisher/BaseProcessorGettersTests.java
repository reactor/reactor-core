package reactor.core.publisher;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Sergey Shcherbakov
 * @author Stephane Maldini
 */
public class BaseProcessorGettersTests {

	@Test
	public void testProcessorTopicGetters() {

		final int TEST_BUFFER_SIZE = 16;
		ProcessorExecutor<Object, Object> processor = ProcessorTopic.create("testProcessor", TEST_BUFFER_SIZE);

		assertEquals(TEST_BUFFER_SIZE, processor.getAvailableCapacity());

		processor.awaitAndShutdown();

	}

	@Test
	public void testProcessorWorkQueueGetters() {

		final int TEST_BUFFER_SIZE = 16;
		ProcessorExecutor<Object, Object> processor = ProcessorWorkQueue.create("testProcessor", TEST_BUFFER_SIZE);

		assertEquals(TEST_BUFFER_SIZE, processor.getAvailableCapacity());

		processor.onNext(new Object());

		assertEquals(TEST_BUFFER_SIZE - 1, processor.getAvailableCapacity());
		processor.awaitAndShutdown();

	}

}
