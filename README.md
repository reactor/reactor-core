# Reactor Core

[![Join the chat at https://gitter.im/reactor/reactor](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/reactor/reactor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Reactor Core](https://maven-badges.herokuapp.com/maven-central/io.projectreactor/reactor-core/badge.svg?style=plastic)](http://mvnrepository.com/artifact/io.projectreactor/reactor-core) 
   [![Download](https://api.bintray.com/packages/spring/jars/io.projectreactor/images/download.svg)](https://bintray.com/spring/jars/io.projectreactor/_latestVersion)
   [![Travis CI](https://travis-ci.org/reactor/reactor-core.svg?branch=master)](https://travis-ci.org/reactor/reactor-core)

Non-Blocking [Reactive Streams](http://reactive-streams.org) Foundation for the JVM both implementing a [Reactive Extensions]
(http://reactivex.io) inspired API and efficient message-passing support.

## Getting it
   
**3.0 requires Java 8 or + to run**.

With Gradle from repo.spring.io or Maven Central repositories (stable releases only):
```groovy
    repositories {
      //maven { url 'http://repo.spring.io/snapshot' }
      mavenCentral()
    }

    dependencies {
      //compile "io.projectreactor:reactor-core:3.0.2.BUILD-SNAPSHOT"
      compile "io.projectreactor:reactor-core:3.0.2.RELEASE"
    }
```

## Getting Started

New to Reactive Programming or bored of reading already ? Try the [Introduction to Reactor Core hands-on](https://github.com/reactor/lite-rx-api-hands-on) !

## Flux

A Reactive Streams Publisher with basic flow operators.
- Static factories on Flux allow for source generation from arbitrary callbacks types.
- Instance methods allows operational building, materialized on each _Flux#subscribe()_, _Flux#subscribe()_ or multicasting operations such as _Flux#publish_ and _Flux#publishNext_.

[<img src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/flux.png" width="500">](http://projectreactor.io/core/docs/api/reactor/core/publisher/Flux.html)

Flux in action :
```java
Flux.fromIterable(getSomeLongList())
    .mergeWith(Flux.interval(100))
    .doOnNext(serviceA::someObserver)
    .map(d -> d * 2)
    .take(3)
    .onErrorResumeWith(errorHandler::fallback)
    .doAfterTerminate(serviceM::incrementTerminate)
    .subscribe(System.out::println);
```

## Mono
A Reactive Streams Publisher constrained to *ZERO* or *ONE* element with appropriate operators. 
- Static factories on Mono allow for deterministic *zero or one* sequence generation from arbitrary callbacks types.
- Instance methods allows operational building, materialized on each _Mono#subscribe()_ or _Mono#get()_ eventually called.

[<img src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/mono.png" width="500">](http://projectreactor.io/core/docs/api/reactor/core/publisher/Mono.html)

Mono in action :
```java
Mono.fromCallable(System::currentTimeMillis)
    .then(time -> Mono.first(serviceA.findRecent(time), serviceB.findRecent(time)))
    .timeout(Duration.ofSeconds(3), errorHandler::fallback)
    .doOnSuccess(r -> serviceM.incrementSuccess())
    .subscribe(System.out::println);
```

Blocking Mono result :
```java    
Tuple2<Long, Long> nowAndLater = 
        Mono.when(
                Mono.just(System.currentTimeMillis()),
                Flux.just(1).delay(1).map(i -> System.currentTimeMillis()))
            .block();
```

## Schedulers

Reactor uses a [Scheduler](http://projectreactor.io/core/docs/api/?reactor/core/scheduler/Scheduler.html) as a
contract for arbitrary task execution. It provides some guarantees required by Reactive
Streams flows like FIFO execution.

You can use or create efficient [schedulers](http://projectreactor.io/core/docs/api/?reactor/core/scheduler/Schedulers.html)
to jump thread on the producing flows (subscribeOn) or receiving flows (publishOn):

```java

Mono.fromCallable( () -> System.currentTimeMillis() )
	.repeat()
    .publishOn(Schedulers.single())
    .log("foo.bar")
    .flatMap(time ->
        Mono.fromCallable(() -> { Thread.sleep(1000); return time; })
            .subscribeOn(Schedulers.parallel())
    , 8) //maxConcurrency 8
    .subscribe();
```

## ParallelFlux

[ParallelFlux](http://projectreactor.io/core/docs/api/?reactor/core/publisher/ParallelFlux
.html) can starve your CPU's from any sequence whose work can be subdivided in concurrent
 tasks. Turn back into a `Flux` with `ParallelFlux#sequential()`, an unordered join or
 use abitrary merge strategies via 'groups()'.

```java
Mono.fromCallable( () -> System.currentTimeMillis() )
	.repeat()
    .parallel(8) //parallelism
    .runOn(Schedulers.parallel())
    .doOnNext( d -> System.out.println("I'm on thread "+Thread.currentThread()) ).
    .sequential()
    .subscribe()
```


## Hot Publishing : SynchronousSink, FluxSink, MonoSink
To bridge a Subscriber or Processor into an outside context that is taking care of
producing non concurrently, use `Flux#create`, `Mono#create`, or
`FluxProcessor#connectSink()`.

```java
Flux.create(emitter -> {
         ActionListener al = e -> {
            emitter.next(textField.getText());
         };

         // without cancellation support:
         button.addActionListener(al);

         // with cancellation support:
         button.addActionListener(al);
         emitter.setCancellation(() -> {
         	button.removeListener(al);
         });
    },
    // Overflow (backpressure) handling, default is BUFFER
    FluxSink.OverflowStrategy.LATEST)
    .timeout(3)
    .doOnComplete(() -> System.out.println("completed!"))
    .subscribe(System.out::println)
```

## Hot Publishing : Processors

The 3 main processor implementations are message relays using 0 ([EmitterProcessor](http://projectreactor.io/core/docs/api/?reactor/core/publisher/EmitterProcessor.html)) or N threads ([TopicProcessor](http://projectreactor.io/core/docs/api/?reactor/core/publisher/TopicProcessor.html) and [WorkQueueProcessor](http://projectreactor.io/core/docs/api/?reactor/core/publisher/WorkQueueProcessor.html)). They also use bounded buffers, aka RingBuffer.

### Pub-Sub : EmitterProcessor

[A signal broadcaster](http://projectreactor.io/core/docs/api/?reactor/core/publisher/EmitterProcessor.html) that will safely handle asynchronous boundaries between N Subscribers (asynchronous or not) and a parent producer.

```java
EmitterProcessor<Integer> emitter = EmitterProcessor.create();
SynchronousSink<Integer> sink = emitter.connectSink();
sink.submit(1);
sink.submit(2);
emitter.subscribe(System.out::println);
sink.submit(3); //output : 3
sink.finish();
```

### Pub-Sub Replay : ReplayProcessor

[A caching broadcaster](http://projectreactor.io/core/docs/api/?reactor/core/publisher/ReplayProcessor.html) that will safely handle
asynchronous boundaries between N Subscribers (asynchronous or not) and a parent producer.

Replay capacity in action:
```java
ReplayProcessor<Integer> replayer = ReplayProcessor.create();
SynchronousSink<Integer> sink = replayer.connectSink();
sink.submit(1);
sink.submit(2);
replayer.subscribe(System.out::println); //output 1, 2
replayer.subscribe(System.out::println); //output 1, 2
sink.submit(3); //output : ...3 ...3
sink.finish();
```

### Async Pub-Sub : TopicProcessor

[An asynchronous signal broadcaster](http://projectreactor.io/core/docs/api/?reactor/core/publisher/TopicProcessor.html) dedicating an event loop thread per subscriber and maxing out producing/consuming rate with temporary tolerance to latency peaks. Also supports multi-producing and emission without onSubscribe.

```java
TopicProcessor<Integer> topic = TopicProcessor.create();
topic.subscribe(System.out::println);
topic.onNext(1); //output : ...1
topic.onNext(2); //output : ...2
topic.subscribe(System.out::println); //output : ...1, 2
topic.onNext(3); //output : ...3 ...3
topic.onComplete();
```

### Async Distributed : WorkQueueProcessor

Similar to TopicProcessor regarding thread per subscriber but this time exclusively distributing the input data signal to the next available Subscriber. [WorkQueueProcessor](http://projectreactor.io/core/docs/api/?reactor/core/publisher/WorkQueueProcessor.html) is also able to replay detected dropped data downstream (error or cancel) to any Subscriber ready.

```java
WorkQueueProcessor<Integer> queue = WorkQueueProcessor.create();
queue.subscribe(System.out::println);
queue.subscribe(System.out::println);
queue.onNext(1); //output : ...1
queue.onNext(2); //output : .... ...2
queue.onNext(3); //output : ...3 
queue.onComplete();
```

## The Backpressure Thing

Most of this cool stuff uses bounded ring buffer implementation under the hood to mitigate signal processing difference between producers and consumers. Now, the operators and processors or any standard reactive stream component working on the sequence will be instructed to flow in when these buffers have free room AND only then. This means that we make sure we both have a deterministic capacity model (bounded buffer) and we never block (request more data on write capacity). Yup, it's not rocket science after all, the boring part is already being worked by us in collaboration with [Reactive Streams Commons](http://github.com/reactor/reactive-streams-commons) on going research effort.

## What's more in it ?

"Operator Fusion" (flow optimizers), health state observers, [TestSubscriber](http://projectreactor.io/core/docs/api/?reactor/test/TestSubscriber.html), helpers to build custom reactive components, bounded queue generator, hash-wheel timer, converters from/to RxJava1, Java 9 Flow.Publisher and Java 8 CompletableFuture.

-------------------------------------
## Reference
http://projectreactor.io/core/docs/reference/ [WIP]

## Javadoc
http://projectreactor.io/core/docs/api/

## Getting started with Flux and Mono
https://github.com/reactor/lite-rx-api-hands-on

## Beyond Reactor Core
- Everything to jump outside the JVM with the non-blocking drivers from [Reactor IPC](http://github.com/reactor/reactor-ipc).
- [Reactor Addons](http://github.com/reactor/reactor-addons) include _Bus_ and _Pipes_ event routers plus a handful of extra reactive modules.

-------------------------------------
_Powered by [Reactive Streams Commons](http://github.com/reactor/reactive-streams-commons)_

_Licensed under [Apache Software License 2.0](www.apache.org/licenses/LICENSE-2.0)_

_Sponsored by [Pivotal](http://pivotal.io)_
