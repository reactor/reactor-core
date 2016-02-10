package reactor.core.publisher;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.test.TestSubscriber;

public class FluxNeverTest {

	@Test
	public void singleInstance() {
		Assert.assertSame(FluxNever.instance(), FluxNever.instance());
	}

	@Test
	public void normal() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();

		FluxNever.<Integer>instance().subscribe(ts);

		ts
		  .assertSubscribed()
		  .assertNoValues()
		  .assertNoError()
		  .assertNotComplete();
	}
}
