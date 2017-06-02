#!/bin/bash

set -ev

if [ "$TRAVIS_PULL_REQUEST" != "false" ]; then
    COMMIT_RANGE="FETCH_HEAD..$TRAVIS_BRANCH"
    echo "travis PR #$TRAVIS_PULL_REQUEST build, looking at files in $COMMIT_RANGE"
    COMMIT_CONTENT=`git diff --name-only $COMMIT_RANGE`
    echo "PR content: $COMMIT_CONTENT"
    echo $COMMIT_CONTENT | grep -qv '^reactor-test/' && {
      echo "something else than reactor-test was touched -> full test"
      ./gradlew check
    } || {
      echo "only reactor-test was touched -> selective test"
      ./gradlew :reactor-test:check
    }
elif [ "$TRAVIS_BRANCH" == "master" ] || [ "$TRAVIS_BRANCH" == "3.0.x" ]; then
    echo "master or 3.0.x: this is a merge test -> full test"
    ./gradlew check
else
    COMMIT_RANGE=${TRAVIS_COMMIT_RANGE/.../..}
    if ! git diff --quiet "$COMMIT_RANGE" -- ; then
        echo "travis commit range diff failed, probably new PR or force push, falling back to single commit"
        COMMIT_CONTENT=`git diff-tree --no-commit-id --name-only -r $TRAVIS_COMMIT`
    else
        echo "travis push build, looking at files in $COMMIT_RANGE"
        COMMIT_CONTENT=`git diff --name-only $COMMIT_RANGE`
    fi
    echo "commits content: $COMMIT_CONTENT"
    echo $COMMIT_CONTENT | grep -qv '^reactor-test/' && {
      echo "something else than reactor-test was touched -> full test"
      ./gradlew check
    } || {
      echo "only reactor-test was touched -> selective test"
      ./gradlew :reactor-test:check
    }
fi

exit 0;
