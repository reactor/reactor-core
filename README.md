# reactor-core

[![Join the chat at https://gitter.im/reactor/reactor](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/reactor/reactor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[![Build Status](https://drone.io/github.com/reactor/reactor-core/status.png)](https://drone.io/github.com/reactor/reactor-core/latest)

Non-Blocking [Reactive Streams](http://reactive-streams.org) Foundation for the JVM both implementing a lite [Reactive Extensions]
(http://reactivex.io) API and efficient message-passing support.

## Getting it

With Gradle from repo.spring.io or Maven Central repositories (stable releases only):
```groovy
    repositories {
      //maven { url 'http://repo.spring.io/libs-release' }
      //maven { url 'http://repo.spring.io/libs-milestone' }
      maven { url 'http://repo.spring.io/libs-snapshot' }
      mavenCentral()
    }

    dependencies {
      compile "io.projectreactor:reactor-core:2.5.0.BUILD-SNAPSHOT"
    }
```

## Flux

A Reactive Streams Publisher with basic Rx operators. 
- Static factories on Flux allow for source generation from arbitrary callbacks types.
- Instance methods allows operational building, materialized on each _Flux#subscribe()_ eventually called.

[<img src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/flux.png" width="500">](http://projectreactor.io/core/docs/api/reactor/core/publisher/Flux.html)

Flux in action :
```java
Flux.fromIterable(getSomeList())
    .mergeWith(Flux.interval(1))
    .map(d -> d * 2)
    .zipWith(Flux.just(1, 2, 3))
    .onErrorResumeWith(errorHandler::fallback)
    .subscribe(Subscribers.consumer(System.out::println));
```

## Mono
A Reactive Streams Publisher constrained to *ZERO* or *ONE* element with appropriate operators. 
- Static factories on Mono allow for deterministic *zero or one* sequence generation from arbitrary callbacks types.
- Instance methods allows operational building, materialized on each _Mono#subscribe()_ or _Mono#get()_ eventually called.

[<img src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/mono.png" width="500">](http://projectreactor.io/core/docs/api/reactor/core/publisher/Mono.html)

Mono in action :
```java
Mono.fromCallable(System::currentTimeMillis)
    .then(time -> Mono.any(serviceA.findRecent(time), serviceB.findRecent(time)))
    .or(Mono.delay(3))
    .otherwiseIfEmpty(Mono.just(errorHandler::fallback)
    .subscribe(Subscribers.consumer(System.out::println));
```

## Schedulers

Create and Reuse scheduling resources over multiple Subscribers, with adapated strategies :

```java
SchedulerGroup async = SchedulerGroup.async();
SchedulerGroup io = SchedulerGroup.io();

Flux.create( sub -> sub.onNext(System.currentTimeMillis()) )
    .dispatchOn(async)
    .log("foo.bar")
    .flatMap(time ->
        Mono.fromCallable(() -> { Thread.sleep(1000); return time; })
            .publishOn(io)
    )
    .subscribe();
```

## Processors

The 3 main processor implementations are message relays using 0 (EmitterProcessor) or N threads (TopicProcessor and WorkQueueProcessor). They also use bounded and replayable buffers, aka RingBuffers, that are optimized for low latency message passing.

### Sync Pub-Sub : EmitterProcessor

A signal broadcaster that will safely handle asynchronous boundaries between N Subscribers (asynchronous or not) and a parent producer.

```java
EmitterProcessor<Integer> emitter = EmitterProcessor.create();
emitter.start();
emitter.onNext(1);
emitter.onNext(2);
emitter.subscribe(Subscriber.consume(System.out::println));
emitter.onNext(3); //output : 3
emitter.onComplete();

EmitterProcessor<Integer> replayer = EmitterProcessor.replay();
replayer.start();
replayer.onNext(1);
replayer.onNext(2);
replayer.subscribe(Subscriber.consume(System.out::println)); //output 1, 2
replayer.onNext(3); //output : 3
replayer.onComplete();
```

### Async Pub-Sub : TopicProcessor

An asynchronous signal broadcaster dedicating a thread per subscriber and maxing out producing/consuming rate with temporary tolerance to latency peaks. Also support multi-producing and emission without onSubscribe.

```java
TopicProcessor<Integer> topic = TopicProcessor.create();
topic.subscribe(Subscriber.consume(System.out::println));
topic.onNext(1); //output : ...1
topic.onNext(2); //output : ...2
topic.subscribe(Subscriber.consume(System.out::println)); //output : ...1, 2
topic.onNext(3); //output : ...3 ...3
topic.onComplete();
```

### Async Distributed : WorkQueueProcessor

```java
WorkQueueProcessor<Integer> queue = WorkQueueProcessor.create();
queue.subscribe(Subscriber.consume(System.out::println));
queue.subscribe(Subscriber.consume(System.out::println));
queue.onNext(1); //output : ...1
queue.onNext(2); //output : .... ...2
queue.onNext(3); //output : ...3 
queue.onComplete();
```

### Hot Publishing : SignalEmitter
Usually, Processors do not support onNext call if onSubscribe has not been called before OR if there is a mismatching demand (onNext without demand). To bridge a Subscriber or Processor into an outside context that is taking care of producing non concurrently, use SignalEmitter.create() or the common FluxProcessor.startEmitter():

```java
EmitterProcessor<String> processor = EmitterProcessor.create();
processor
    .doOnNext(System.out::println)
    .subscribe();

SignalEmitter<String> sink = processor.startEmitter();
Emission status = sink.emit("Non blocking and returning emission status");
long latency = sink.submit("Blocking until emitted and returning latency");
sink.onNext("Fail if overrun");
sink.finish();

```

## The Backpressure Thing

```java
```

## What's more in it ?

"Operator Fusion" (flow optimizers), health state observers, helpers to build custom reactive components, bounded queue generator, hash-wheel timer, converters from/to RxJava1, Java 9 Flow.Publisher and Java 8 CompletableFuture.

## Reference
http://projectreactor.io/core/docs/reference/

## Javadoc
http://projectreactor.io/core/docs/api/

-------------------------------------
_Powered by [Reactive Stream Commons](http://github.com/reactor/reactive-streams-commons)_

_Licensed under [Apache Software License 2.0](www.apache.org/licenses/LICENSE-2.0)_

_Sponsored by [Pivotal](http://pivotal.io)_
