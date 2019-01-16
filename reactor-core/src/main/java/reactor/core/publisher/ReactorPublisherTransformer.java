package reactor.core.publisher;

import java.util.function.Function;

import org.reactivestreams.Publisher;

/**
 * Helper visitor to apply a transformation on each known {@link Publisher} type implemented by Reactor.
 *
 * @author Sergei @bsideup Egorov
 */
public interface ReactorPublisherTransformer
		extends Function<Publisher<Object>, Publisher<Object>> {

	Mono<Object> transform(Mono<Object> source);

	Flux<Object> transform(Flux<Object> source);

	ParallelFlux<Object> transform(ParallelFlux<Object> source);

	Publisher<Object> transform(Publisher<Object> source);

	@Override
	default Publisher<Object> apply(Publisher<Object> publisher) {
		if (publisher instanceof Mono) {
			return transform((Mono<Object>) publisher);
		}

		if (publisher instanceof Flux) {
			return transform((Flux<Object>) publisher);
		}

		if (publisher instanceof ParallelFlux) {
			return transform((ParallelFlux<Object>) publisher);
		}

		return transform(publisher);
	}

}
