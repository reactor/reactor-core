# Reactor Core

[![Join the chat at https://gitter.im/reactor/reactor](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/reactor/reactor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Reactor Core](https://maven-badges.herokuapp.com/maven-central/io.projectreactor/reactor-core/badge.svg?style=plastic)](http://mvnrepository.com/artifact/io.projectreactor/reactor-core) [![Latest](https://img.shields.io/github/release/reactor/reactor-core/all.svg)]() 

[![Download](https://api.bintray.com/packages/spring/jars/io.projectreactor/images/download.svg)](https://bintray.com/spring/jars/io.projectreactor/_latestVersion)

[![Travis CI](https://travis-ci.org/reactor/reactor-core.svg?branch=master)](https://travis-ci.org/reactor/reactor-core)
[![Codecov](https://img.shields.io/codecov/c/github/reactor/reactor-core.svg)]()


Non-Blocking [Reactive Streams](http://reactive-streams.org) Foundation for the JVM both implementing a [Reactive Extensions](http://reactivex.io) inspired API and efficient event streaming support.

## Getting it
   
**Reactor 3 requires Java 8 or + to run**.

With Gradle from repo.spring.io or Maven Central repositories (stable releases only):

```groovy
    repositories {
      //maven { url 'http://repo.spring.io/snapshot' }
      //maven { url 'http://repo.spring.io/milestone' }
      mavenCentral()
    }

    dependencies {
      //compile "io.projectreactor:reactor-core:3.1.16.BUILD-SNAPSHOT"
      //testCompile("io.projectreactor:reactor-test:3.1.16.BUILD-SNAPSHOT")
      compile "io.projectreactor:reactor-core:3.1.15.RELEASE"
      testCompile("io.projectreactor:reactor-test:3.1.15.RELEASE")
    }
```

See the [reference documentation](http://projectreactor.io/docs/core/release/reference/docs/index.html#getting)
for more information on getting it (eg. using Maven, or on how to get milestones and snapshots).

> **Note about Android support**: Reactor 3 doesn't officially support nor target Android.
However it should work fine with Android SDK 26 (Android O) and above. See the
[complete note](http://projectreactor.io/docs/core/release/reference/docs/index.html#prerequisites)
in the reference guide.


## Getting Started

New to Reactive Programming or bored of reading already ? Try the [Introduction to Reactor Core hands-on](https://github.com/reactor/lite-rx-api-hands-on) !

If you are familiar with RxJava or if you want to check more detailled introduction, be sure to check 
https://www.infoq.com/articles/reactor-by-example !

## Flux

A Reactive Streams Publisher with basic flow operators.
- Static factories on Flux allow for source generation from arbitrary callbacks types.
- Instance methods allows operational building, materialized on each _Flux#subscribe()_, _Flux#subscribe()_ or multicasting operations such as _Flux#publish_ and _Flux#publishNext_.

[<img src="https://raw.githubusercontent.com/reactor/reactor-core/v3.1.3.RELEASE/src/docs/marble/flux.png" width="500">](http://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html)

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

[<img src="https://raw.githubusercontent.com/reactor/reactor-core/v3.1.3.RELEASE/src/docs/marble/mono.png" width="500">](http://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html)

Mono in action :
```java
Mono.fromCallable(System::currentTimeMillis)
    .flatMap(time -> Mono.first(serviceA.findRecent(time), serviceB.findRecent(time)))
    .timeout(Duration.ofSeconds(3), errorHandler::fallback)
    .doOnSuccess(r -> serviceM.incrementSuccess())
    .subscribe(System.out::println);
```

Blocking Mono result :
```java    
Tuple2<Long, Long> nowAndLater = 
        Mono.zip(
                Mono.just(System.currentTimeMillis()),
                Flux.just(1).delay(1).map(i -> System.currentTimeMillis()))
            .block();
```

## Schedulers

Reactor uses a [Scheduler](http://projectreactor.io/docs/core/release/api/reactor/core/scheduler/Scheduler.html) as a
contract for arbitrary task execution. It provides some guarantees required by Reactive
Streams flows like FIFO execution.

You can use or create efficient [schedulers](http://projectreactor.io/docs/core/release/api/reactor/core/scheduler/Schedulers.html)
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

[ParallelFlux](http://projectreactor.io/docs/core/release/api/reactor/core/publisher/ParallelFlux.html) can starve your CPU's from any sequence whose work can be subdivided in concurrent
 tasks. Turn back into a `Flux` with `ParallelFlux#sequential()`, an unordered join or
 use abitrary merge strategies via 'groups()'.

```java
Mono.fromCallable( () -> System.currentTimeMillis() )
	.repeat()
    .parallel(8) //parallelism
    .runOn(Schedulers.parallel())
    .doOnNext( d -> System.out.println("I'm on thread "+Thread.currentThread()) )
    .subscribe()
```


## Custom sources : Flux.create and FluxSink, Mono.create and MonoSink
To bridge a Subscriber or Processor into an outside context that is taking care of
producing non concurrently, use `Flux#create`, `Mono#create`.

```java
Flux.create(sink -> {
         ActionListener al = e -> {
            sink.next(textField.getText());
         };

         // without cancellation support:
         button.addActionListener(al);

         // with cancellation support:
         sink.onCancel(() -> {
         	button.removeListener(al);
         });
    },
    // Overflow (backpressure) handling, default is BUFFER
    FluxSink.OverflowStrategy.LATEST)
    .timeout(3)
    .doOnComplete(() -> System.out.println("completed!"))
    .subscribe(System.out::println)
```

## The Backpressure Thing

Most of this cool stuff uses bounded ring buffer implementation under the hood to mitigate signal processing difference between producers and consumers. Now, the operators and processors or any standard reactive stream component working on the sequence will be instructed to flow in when these buffers have free room AND only then. This means that we make sure we both have a deterministic capacity model (bounded buffer) and we never block (request more data on write capacity). Yup, it's not rocket science after all, the boring part is already being worked by us in collaboration with [Reactive Streams Commons](http://github.com/reactor/reactive-streams-commons) on going research effort.

## What's more in it ?

"Operator Fusion" (flow optimizers), health state observers, helpers to build custom reactive components, bounded queue generator, hash-wheel timer, converters from/to Java 9 Flow, Publisher and Java 8 CompletableFuture. The repository contains a `reactor-test` project with test features like the [`StepVerifier`](http://projectreactor.io/docs/test/release/api/index.html?reactor/test/StepVerifier.html).

-------------------------------------

## Reference Guide
http://projectreactor.io/docs/core/release/reference/docs/index.html

## Javadoc
https://projectreactor.io/docs/core/release/api/

## Getting started with Flux and Mono
https://github.com/reactor/lite-rx-api-hands-on

## Reactor By Example
https://www.infoq.com/articles/reactor-by-example

## Beyond Reactor Core
- Everything to jump outside the JVM with the non-blocking drivers from [Reactor IPC](http://github.com/reactor/reactor-ipc).
- [Reactor Addons](http://github.com/reactor/reactor-addons) provide for adapters and extra operators for Reactor 3.

-------------------------------------
_Powered by [Reactive Streams Commons](http://github.com/reactor/reactive-streams-commons)_

_Licensed under [Apache Software License 2.0](www.apache.org/licenses/LICENSE-2.0)_

_Sponsored by [Pivotal](http://pivotal.io)_

