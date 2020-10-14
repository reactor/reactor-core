package reactor.core.publisher;

import org.junit.jupiter.api.Test;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxErrorOnRequestTest {

	@Test
	public void scanOperator(){
	    FluxErrorOnRequest test = new FluxErrorOnRequest(new IllegalStateException());

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	    assertThat(test.scan(Scannable.Attr.ACTUAL)).isNull();
	}

	@Test
	public void scanSubscription() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, sub -> sub.request(100));
		IllegalStateException error = new IllegalStateException();
		FluxErrorOnRequest.ErrorSubscription test = new FluxErrorOnRequest.ErrorSubscription(actual, error);

		assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(error);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}