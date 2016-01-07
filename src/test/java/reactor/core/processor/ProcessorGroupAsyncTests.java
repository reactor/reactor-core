package reactor.core.processor;

import org.junit.Test;
import org.reactivestreams.Processor;
import org.testng.SkipException;
import reactor.Processors;
import reactor.core.support.Assert;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @author Anatoly Kadyshev
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class ProcessorGroupAsyncTests extends AbstractProcessorVerification {

	private final int           BUFFER_SIZE     = 8;
	private final AtomicBoolean exceptionThrown = new AtomicBoolean();
	private final int           N               = 17;

	@Override
	public Processor<Long, Long> createProcessor(int bufferSize) {
		return Processors.<Long>singleGroup("shared-async", bufferSize, Throwable::printStackTrace).get();
	}

	@Override
	public void required_spec104_mustCallOnErrorOnAllItsSubscribersIfItEncountersANonRecoverableError() throws
	  Throwable {
		throw new SkipException("Optional requirement");
	}
/*
	@Override
	public void required_mustRequestFromUpstreamForElementsThatHaveBeenRequestedLongAgo() throws Throwable {
		throw new SkipException("Optional multi subscribe requirement");
	}*/

	@Override
	public long maxSupportedSubscribers() {
		return 1L;
	}

	@Test
	public void testDispatch() throws InterruptedException {
		ProcessorGroup<String> service = Processors.singleGroup("dispatcher", BUFFER_SIZE, t -> {
			exceptionThrown.set(true);
			t.printStackTrace();
		});

		ProcessorGroup.release(
		  runTest(service.dataDispatcher()),
		  runTest(service.dataDispatcher())
		);
	}

	private BiConsumer<String, Consumer<? super String>> runTest(final BiConsumer<String, Consumer<? super String>>
	                                                               dispatcher) throws InterruptedException {
		CountDownLatch tasksCountDown = new CountDownLatch(N);

		dispatcher.accept("Hello", s -> {
			for (int i = 0; i < N; i++) {
				dispatcher.accept("world", s1 -> tasksCountDown.countDown());
			}
		});

		boolean check = tasksCountDown.await(5, TimeUnit.SECONDS);
		Assert.isTrue(!exceptionThrown.get());
		Assert.isTrue(check);

		return dispatcher;
	}

}