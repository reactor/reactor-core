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
[<img src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/flux.png" width="500">](http://next.projectreactor.io/core/docs/api/reactor/core/publisher/Flux.html)

```java
```

## Mono
[<img src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/mono.png" width="500">](http://next.projectreactor.io/core/docs/api/reactor/core/publisher/Mono.html)

```java
```

## Processors

### Sync Pub-Sub : EmitterProcessor

```java
```

### Async Pub-Sub : TopicProcessor

```java
```

### Async Distributed : WorkQueueProcessor

```java
```

### TopicProcessor

```java
```

### Hot Publishing
Hint: it's not porn.

```java
```

## Schedulers

```java
```

## The Backpressure Thing

```java
```

## What's more in it ?

"Operator Fusion",

## Reference
http://next.projectreactor.io/core/docs/reference/

## Javadoc
http://next.projectreactor.io/core/docs/api/

Powered by [Reactive Stream Commons](http://github.com/reactor/reactive-streams-commons)
