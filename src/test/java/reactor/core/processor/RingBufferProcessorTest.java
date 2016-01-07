package reactor.core.processor;

import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.subscriber.test.TestSubscriber;
import reactor.io.buffer.Buffer;

/**
 * @author Anatoly Kadyshev
 */
public class RingBufferProcessorTest {

	@Test
	@Ignore
	public void required_spec209_mustBePreparedToReceiveAnOnCompleteSignalWithoutPrecedingRequestCall()
			throws InterruptedException {
		RingBufferProcessor<String> processor = RingBufferProcessor.create();
		Publisher<String> publisher = Subscriber::onComplete;
		publisher.subscribe(processor);

		// Waiting till publisher sends Complete into the processor
		Thread.sleep(1000);

		TestSubscriber<String> subscriber = TestSubscriber.createWithTimeoutSecs(1);
		processor.subscribe(subscriber);

		subscriber.assertCompleteReceived();
	}

}
