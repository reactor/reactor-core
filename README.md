# Reactor Core

[![Join the chat at https://gitter.im/reactor/reactor](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/reactor/reactor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Reactor Core](https://maven-badges.herokuapp.com/maven-central/io.projectreactor/reactor-core/badge.svg?style=plastic)](https://mvnrepository.com/artifact/io.projectreactor/reactor-core) [![Latest](https://img.shields.io/github/release/reactor/reactor-core/all.svg)]() 

[![CI on GHA](https://github.com/reactor/reactor-core/actions/workflows/publish.yml/badge.svg)](https://github.com/reactor/reactor-core/actions/workflows/publish.yml)
[![Codecov](https://img.shields.io/codecov/c/github/reactor/reactor-core.svg)]()
[![Code Quality: Java](https://img.shields.io/lgtm/grade/java/g/reactor/reactor-core.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/reactor/reactor-core/context:java)
[![Total Alerts](https://img.shields.io/lgtm/alerts/g/reactor/reactor-core.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/reactor/reactor-core/alerts)


Non-Blocking [Reactive Streams](https://www.reactive-streams.org/) Foundation for the JVM both implementing a [Reactive Extensions](https://reactivex.io) inspired API and efficient event streaming support.

Since `3.3.x`, this repository also contains `reactor-tools`, a java agent aimed at helping with debugging of Reactor code.

## Getting it
   
**Reactor 3 requires Java 8 or + to run**.

With Gradle from repo.spring.io or Maven Central repositories (stable releases only):

```groovy
repositories {
    mavenCentral()

    // Uncomment to get access to Milestones
    // maven { url "https://repo.spring.io/milestone" }

    // Uncomment to get access to Snapshots
    // maven { url "https://repo.spring.io/snapshot" }
}

dependencies {
    compile "io.projectreactor:reactor-core:3.7.0-M5"
    testCompile "io.projectreactor:reactor-test:3.7.0-M5"

    // Alternatively, use the following for latest snapshot artifacts in this line
    // compile "io.projectreactor:reactor-core:3.7.0-SNAPSHOT"
    // testCompile "io.projectreactor:reactor-test:3.7.0-SNAPSHOT"

    // Optionally, use `reactor-tools` to help debugging reactor code
    // implementation "io.projectreactor:reactor-tools:3.7.0-M5"
}
```

See the [reference documentation](https://projectreactor.io/docs/core/release/reference/docs/index.html#getting)
for more information on getting it (eg. using Maven, or on how to get milestones and snapshots).

> **Note about Android support**: Reactor 3 doesn't officially support nor target Android.
However it should work fine with Android SDK 21 (Android 5.0) and above. See the
[complete note](https://projectreactor.io/docs/core/release/reference/docs/index.html#prerequisites)
in the reference guide.

## Trouble building the project?
Since the introduction of [Java Multi-Release JAR File](https://openjdk.org/jeps/238) 
support one needs to have JDK 8, 9, and 21 available on the classpath. All the JDKs should 
be automatically [detected](https://docs.gradle.org/current/userguide/toolchains.html#sec:auto_detection) 
or [provisioned](https://docs.gradle.org/current/userguide/toolchains.html#sec:provisioning) 
by Gradle Toolchain. 

However, if you see error message such as `No matching toolchains found for requested 
specification: {languageVersion=X, vendor=any, implementation=vendor-specific}` (where 
`X` can be 8, 9 or 21), it means that you need to install the missing JDK:

### Installing JDKs with [SDKMAN!](https://sdkman.io/)

In the project root folder run [SDKMAN env initialization](https://sdkman.io/usage#env): 

```shell
sdk env install
```

then (if needed) install JDK 9: 

```shell
sdk install java $(sdk list java | grep -Eo -m1 '9\b\.[ea|0-9]{1,2}\.[0-9]{1,2}-open')
```

then (if needed) install JDK 21:

```shell
sdk install java $(sdk list java | grep -Eo -m1 '21\b\.[ea|0-9]{1,2}\.[0-9]{1,2}-open')
```

When the installations succeed, try to refresh the project and see that it builds.

### Installing JDKs manually

The manual Operation-system specific JDK installation
is well explained in the [official docs](https://docs.oracle.com/en/java/javase/20/install/overview-jdk-installation.html)

### Building the doc

The current active shell JDK version must be compatible with JDK17 or higher for Antora to build successfully.
So, just ensure that you have installed JDK 21, as described above and make it as the current one. 

Then you can build the antora documentation like this:
```shell
./gradlew docs
```

The documentation is generated in `docs/build/site/index.html` and in `docs/build/distributions/reactor-core-<version>-docs.zip` 
If a PDF file should also be included in the generated docs zip file, then you need to specify `-PforcePdf` option:

```shell
./gradlew docs -PforcePdf
```
Notice that PDF generation requires the `asciidoctor-pdf` command to be available in the PATH. 
For example, on Mac OS, you can install such command like this:

```shell
brew install asciidoctor
```

## Getting Started

New to Reactive Programming or bored of reading already ? Try the [Introduction to Reactor Core hands-on](https://github.com/reactor/lite-rx-api-hands-on) !

If you are familiar with RxJava or if you want to check more detailed introduction, be sure to check 
https://www.infoq.com/articles/reactor-by-example !

## Flux

A Reactive Streams Publisher with basic flow operators.
- Static factories on Flux allow for source generation from arbitrary callbacks types.
- Instance methods allows operational building, materialized on each subscription (_Flux#subscribe()_, ...) or multicasting operations (such as _Flux#publish_ and _Flux#publishNext_).

[<img src="https://raw.githubusercontent.com/reactor/reactor-core/v3.1.3.RELEASE/src/docs/marble/flux.png" width="500" style="background-color: white">](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html)

Flux in action :
```java
Flux.fromIterable(getSomeLongList())
    .mergeWith(Flux.interval(100))
    .doOnNext(serviceA::someObserver)
    .map(d -> d * 2)
    .take(3)
    .onErrorResume(errorHandler::fallback)
    .doAfterTerminate(serviceM::incrementTerminate)
    .subscribe(System.out::println);
```

## Mono
A Reactive Streams Publisher constrained to *ZERO* or *ONE* element with appropriate operators. 
- Static factories on Mono allow for deterministic *zero or one* sequence generation from arbitrary callbacks types.
- Instance methods allows operational building, materialized on each _Mono#subscribe()_ or _Mono#get()_ eventually called.

[<img src="https://raw.githubusercontent.com/reactor/reactor-core/v3.4.1/reactor-core/src/main/java/reactor/core/publisher/doc-files/marbles/mono.svg" width="500" style="background-color: white">](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html)

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
Tuple2<Instant, Instant> nowAndLater = Mono.zip(
    Mono.just(Instant.now()),
    Mono.delay(Duration.ofSeconds(1)).then(Mono.fromCallable(Instant::now)))
  .block();
```

## Schedulers

Reactor uses a [Scheduler](https://projectreactor.io/docs/core/release/api/reactor/core/scheduler/Scheduler.html) as a
contract for arbitrary task execution. It provides some guarantees required by Reactive
Streams flows like FIFO execution.

You can use or create efficient [schedulers](https://projectreactor.io/docs/core/release/api/reactor/core/scheduler/Schedulers.html)
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

[ParallelFlux](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/ParallelFlux.html) can starve your CPU's from any sequence whose work can be subdivided in concurrent
 tasks. Turn back into a `Flux` with `ParallelFlux#sequential()`, an unordered join or
 use arbitrary merge strategies via 'groups()'.

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
    .timeout(Duration.ofSeconds(3))
    .doOnComplete(() -> System.out.println("completed!"))
    .subscribe(System.out::println)
```

## The Backpressure Thing

Most of this cool stuff uses bounded ring buffer implementation under the hood to mitigate signal processing difference between producers and consumers. Now, the operators and processors or any standard reactive stream component working on the sequence will be instructed to flow in when these buffers have free room AND only then. This means that we make sure we both have a deterministic capacity model (bounded buffer) and we never block (request more data on write capacity). Yup, it's not rocket science after all, the boring part is already being worked by us in collaboration with [Reactive Streams Commons](https://github.com/reactor/reactive-streams-commons) on going research effort.

## What's more in it ?

"Operator Fusion" (flow optimizers), health state observers, helpers to build custom reactive components, bounded queue generator, converters from/to Java 9 Flow, Publisher and Java 8 CompletableFuture. The repository contains a `reactor-test` project with test features like the [`StepVerifier`](https://projectreactor.io/docs/test/release/api/index.html?reactor/test/StepVerifier.html).

-------------------------------------

## Reference Guide
https://projectreactor.io/docs/core/release/reference/docs/index.html

## Javadoc
https://projectreactor.io/docs/core/release/api/

## Getting started with Flux and Mono
https://github.com/reactor/lite-rx-api-hands-on

## Reactor By Example
https://www.infoq.com/articles/reactor-by-example

## Head-First Spring & Reactor
https://github.com/reactor/head-first-reactive-with-spring-and-reactor/

## Beyond Reactor Core
- Everything to jump outside the JVM with the non-blocking drivers from [Reactor Netty](https://github.com/reactor/reactor-netty).
- [Reactor Addons](https://github.com/reactor/reactor-addons) provide for adapters and extra operators for Reactor 3.

-------------------------------------
_Powered by [Reactive Streams Commons](https://github.com/reactor/reactive-streams-commons)_

_Licensed under [Apache Software License 2.0](https://www.apache.org/licenses/LICENSE-2.0)_

_Sponsored by [VMware](https://tanzu.vmware.com/)_
