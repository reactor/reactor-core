package reactor.core.loom;

import java.util.function.Function;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

public class LoomSupport {

	static <T, R> Function<Flux<T>, Flux<R>> mapAsync(Function<T, R> mapper) {
		return mapAsync(mapper, Queues.SMALL_BUFFER_SIZE);
	}

	 static <T, R> Function<Flux<T>, Flux<R>> mapAsync(Function<T, R> mapper, int concurrency) {
		 return f -> f.flatMap((v) -> Mono.fromCallable(() -> mapper.apply(v))
		                                  .subscribeOn(Schedulers.boundedElastic()), concurrency);
	 }
}
