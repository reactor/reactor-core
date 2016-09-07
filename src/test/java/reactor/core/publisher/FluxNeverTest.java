package reactor.core.publisher;

import org.junit.Assert;
import org.junit.Test;
import reactor.test.TestSubscriber;

public class FluxNeverTest {

	@Test
	public void singleInstance() {
		Assert.assertSame(Flux.never(), Flux.never());
	}

	@Test
	public void normal() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		FluxNever.<Integer>instance().subscribe(ts);

		ts
		  .assertSubscribed()
		  .assertNoValues()
		  .assertNoError()
		  .assertNotComplete();
	}
}
