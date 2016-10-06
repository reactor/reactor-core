package reactor.core.publisher;

import org.junit.Assert;
import org.junit.Test;
import reactor.test.subscriber.AssertSubscriber;

public class FluxNeverTest {

	@Test
	public void singleInstance() {
		Assert.assertSame(Flux.never(), Flux.never());
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		FluxNever.<Integer>instance().subscribe(ts);

		ts
		  .assertSubscribed()
		  .assertNoValues()
		  .assertNoError()
		  .assertNotComplete();
	}
}
