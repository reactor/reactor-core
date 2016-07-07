package reactor.core.converter;

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.publisher.Mono;
import reactor.core.util.BackpressureUtils;
import rx.*;
import rx.Completable.CompletableSubscriber;

/**
 * Convert a {@link Publisher Publisher} into a Completable
 *
 * @author Joao Pedro Evangelista
 * @author Sebastien Deleuze
 * @since 2.5
 */
public enum RxJava1CompletableConverter {
    ;

    public static Mono<Void> toPublisher(Completable completable) {
        return new CompletableAsMono(completable);
    }

    public static Completable fromPublisher(Publisher<?> source) {
        return Completable.create(new PublisherAsCompletable(source));
    }

    /**
     * Wraps an rx.Completable and exposes it as a Mono&lt;Void>.
     */
    private static class CompletableAsMono extends Mono<Void> {
        final Completable source;
        
        public CompletableAsMono(Completable source) {
            this.source = Objects.requireNonNull(source, "source");
        }
        
        @Override
        public void subscribe(Subscriber<? super Void> s) {
            source.subscribe(new CompletableMonoSubscriber(s));
        }
        
        static final class CompletableMonoSubscriber 
        implements Completable.CompletableSubscriber, Subscription {
            final Subscriber<? super Void> actual;

            rx.Subscription d;
            
            public CompletableMonoSubscriber(Subscriber<? super Void> actual) {
                this.actual = actual;
            }
            
            @Override
            public void onSubscribe(rx.Subscription d) {
                Objects.requireNonNull(d, "rx.Subscription cannot be null!");
                if (this.d != null) {
                    d.unsubscribe();
                    return;
                }
                this.d = d;
                
                actual.onSubscribe(this);
            }
            
            @Override
            public void onError(Throwable e) {
                actual.onError(e);
            }
            
            @Override
            public void onCompleted() {
                actual.onComplete();
            }
            
            @Override
            public void request(long n) {
                // ignored, Completable never returns a value
            }
            
            @Override
            public void cancel() {
                d.unsubscribe();
            }
        }
    }
    
    /**
     * Wraps a Publisher and exposes it as a CompletableOnSubscribe and ignores
     * the onNext signals of the Publisher.
     */
    private static class PublisherAsCompletable implements Completable.CompletableOnSubscribe {
        final Publisher<?> source;
        
        public PublisherAsCompletable(Publisher<?> source) {
            this.source = Objects.requireNonNull(source, "source");
        }
        
        @Override
        public void call(CompletableSubscriber t) {
            source.subscribe(new PublisherCompletableSubscriber(t));
        }
        
        static final class PublisherCompletableSubscriber 
        implements Subscriber<Object>, rx.Subscription {
            final CompletableSubscriber actual;

            Subscription s;
            
            volatile boolean unsubscribed;
            
            public PublisherCompletableSubscriber(CompletableSubscriber actual) {
                this.actual = actual;
            }
            
            @Override
            public void onSubscribe(Subscription s) {
                if (BackpressureUtils.validate(this.s, s)) {
                    this.s = s;
                    
                    actual.onSubscribe(this);
                    
                    s.request(Long.MAX_VALUE);
                }
            }
            
            @Override
            public void onNext(Object t) {
                // deliberately ignoring events
            }
            
            @Override
            public void onError(Throwable t) {
                actual.onError(t);
            }
            
            @Override
            public void onComplete() {
                actual.onCompleted();
            }
            
            @Override
            public boolean isUnsubscribed() {
                return unsubscribed;
            }
            
            @Override
            public void unsubscribe() {
                if (unsubscribed) {
                    return;
                }
                unsubscribed = true;
                s.cancel();
            }
        }
    }
}
