package reactor.core.converter;

import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import reactor.core.publisher.Mono;
import reactor.core.test.TestSubscriber;
import rx.Completable;
import rx.Observable;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * @author Joao Pedro Evangelista
 */
public class RxJavaCompletableTests {

    private Completable completable;

    private RxJava1CompletableConverter converter;

    @Before
    public void setUp() throws Exception {
        completable = Completable.fromObservable(Observable.just(1,2,3,4,5));
        converter = RxJava1CompletableConverter.INSTANCE;
    }

    @Test
    public void shouldConvertACompletableIntoMonoVoid() throws Exception {
        Mono<Void> printableMono = converter.toPublisher(completable);
        assertThat(printableMono, is(notNullValue()));
        TestSubscriber
                .subscribe(printableMono)
                .assertNoValues()
                .assertComplete();
    }


    @Test
    public void completableWithErrorIntoMono() throws Exception {
        Mono<Void> voidMono = converter.toPublisher(Completable.fromAction(() -> {
            throw new IllegalStateException("This should not happen"); //something internal happened
        }));
        TestSubscriber
                .subscribe(voidMono)
                .assertNoValues()
                .assertError(IllegalStateException.class)
                .assertTerminated()
                .assertNotComplete();
    }

    @Test
    public void shouldConvertAMonoIntoCompletable() throws Exception {
        Completable completable = RxJava1CompletableConverter.from(Mono.just(1));
        Throwable maybeErrors = completable.get();
        assertThat(maybeErrors, is(nullValue()));
    }
}
