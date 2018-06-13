#!/bin/bash

set -ev

for i in 1 2 3
do
    sleep 300;
    echo "=====[ $((i * 5))min, still running ]=====";
done &

if [ "$TRAVIS_PULL_REQUEST" != "false" ]; then
    COMMIT_RANGE="FETCH_HEAD..$TRAVIS_BRANCH"
    echo "travis PR #$TRAVIS_PULL_REQUEST build, looking at files in $COMMIT_RANGE"
    COMMIT_CONTENT=`git diff --name-only $COMMIT_RANGE`
    echo "PR content: $COMMIT_CONTENT"
    if echo $COMMIT_CONTENT | grep -qv '^reactor-test/' ; then
      echo "something else than reactor-test was touched -> full test"
      ./gradlew check
    else
      echo "only reactor-test was touched -> selective test"
      ./gradlew :reactor-test:check
    fi
elif [ "$TRAVIS_BRANCH" == "master" ] || [ "$TRAVIS_BRANCH" == "3.0.x" ]; then
    echo "master or 3.0.x: this is a merge test -> full test"
    ./gradlew check
else
    COMMIT_RANGE=${TRAVIS_COMMIT_RANGE/.../..}
    echo "travis push build, looking at files in $COMMIT_RANGE"
    if [ "$COMMIT_RANGE" == "" ]; then
      echo "travis commit range empty, probably first push to a new branch"
      COMMIT_CONTENT=`git diff-tree --no-commit-id --name-only -r $TRAVIS_COMMIT`
    else
      COMMIT_CONTENT=`git diff --name-only $COMMIT_RANGE` || {
        echo "travis commit range diff failed, probably new PR or force push, falling back to single commit $TRAVIS_COMMIT"
        COMMIT_CONTENT=`git diff-tree --no-commit-id --name-only -r $TRAVIS_COMMIT`
      }
    fi
    echo "commits content: $COMMIT_CONTENT"
    if echo $COMMIT_CONTENT | grep -qv '^reactor-test/' ; then
      echo "something else than reactor-test was touched -> full test"
      ./gradlew check
    else
      echo "only reactor-test was touched -> selective test"
      ./gradlew :reactor-test:check
    fi
fi

kill %1

exit 0;
