package reactor.core.converter;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Single;

/**
 * Convert a {@link Publisher Publisher} into a Completable
 *
 * @author Joao Pedro Evangelista
 * @since 2.5
 */
public class RxJava1CompletableConverter extends PublisherConverter<Completable> {

    static final RxJava1CompletableConverter INSTANCE = new RxJava1CompletableConverter();

    @SuppressWarnings("TypeMayBeWeakened")
    static Completable from(Publisher<?> noValue) {
        return INSTANCE.fromPublisher(noValue);
    }

    static Mono<Void> from(Completable completable) {
        return INSTANCE.toPublisher(completable);
    }


    @SuppressWarnings("unchecked")
    @Override
    protected Mono<Void> toPublisher(Object o) {
        if (o instanceof Completable) {
            Publisher<?> publisher = DependencyUtils.<Void>convertToPublisher(((Completable) o).<Void>toObservable());
            return Mono.from((Publisher<? extends Void>) publisher);
        } else {
            return null;
        }
    }

    @Override
    protected Completable fromPublisher(Publisher<?> source) {
        return Completable.fromSingle(DependencyUtils.convertFromPublisher(source, Single.class));
    }

    @Override
    public Class<Completable> get() {
        return Completable.class;
    }
}
