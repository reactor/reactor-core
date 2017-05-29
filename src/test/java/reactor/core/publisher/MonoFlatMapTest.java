package reactor.core.publisher;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoFlatMapTest {

	@Test
	public void normalHidden() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1).hide().flatMap(v -> Mono.just(2).hide()).subscribe(ts);

		ts.assertValues(2)
		.assertComplete()
		.assertNoError();
	}

	@Test
	public void cancel() {
		TestPublisher<String> cancelTester = TestPublisher.create();

		MonoProcessor<Integer> processor = cancelTester.mono()
													   .flatMap(s -> Mono.just(s.length()))
													   .toProcessor();
		processor.subscribe();
		processor.cancel();

		cancelTester.assertCancelled();
	}

	@Test
	public void scanMain() {
		Subscriber<Integer> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoFlatMap.ThenMapMain<String, Integer> test = new MonoFlatMap.ThenMapMain<>(
				actual, s -> Mono.just(s.length()));
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}

	@Test
	public void scanInner() {
		Subscriber<Integer> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoFlatMap.ThenMapMain<String, Integer> main = new MonoFlatMap.ThenMapMain<>(actual, s -> Mono.just(s.length()));
		MonoFlatMap.ThenMapInner<Integer> test = new MonoFlatMap.ThenMapInner<>(main);
		Subscription innerSubscription = Operators.emptySubscription();
		test.onSubscribe(innerSubscription);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(innerSubscription);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(main);
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();

		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();

		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}
}
