# Welcome to Reactor's Contributor guide!

### Understand the basics

Not sure what a pull request is, or how to submit one? Take a look at GitHub's
excellent [help documentation][] first.

### Search Stack Overflow first; discuss if necessary

If you're unsure why something isn't working or wondering if there is a better
way of doing it please check on Stack Overflow first and if necessary start
a discussion.

### Search Github Issues; create an issue if necessary

Is there already an issue that addresses your concern? Do a bit of searching
in our [Issues][] to see if you can find something similar. If you
do not find something similar, please create a new issue before submitting
a pull request unless the change is truly trivial -- for example: typo fixes,
removing compiler warnings, etc.

### Sign the Individual Contributor License Agreement (ICLA)

If you have not previously done so, please sign the [Spring CLA form](https://cla.pivotal.io/sign/spring).

It integrates with Github, just follow the steps. Actually, when you first submit a
Pull Request a bot will comment on your PR if you haven't signed the CLA (and if you have,
you'll get a nice green checkmark to the PR checks).

## Create a Branch

### Branch from `master`

Master currently represents work toward Reactor 3.1. Please submit
all pull requests there, even bug fixes and minor improvements. Backports to
will be considered on a case-by-case basis.


### Use short branch names

Branches used when submitting pull requests should preferably be named
according to Github issues, e.g. 'gh-1234'. Otherwise, use succinct, lower-case,
dash (-) delimited names, such as 'fix-warnings', 'fix-typo', etc. In
[fork-and-edit][] cases, the GitHub default 'patch-1' is fine as well. This is
important, because branch names show up in the merge commits that result from
accepting pull requests and should be as expressive and concise as possible.

## Use Spring Framework Code Style

Using your IDE code style support:

Eclipse users can import the content of `$PROJECT_DIR/codequality/eclipse`

IntelliJ users can import the xml from `$PROJECT_DIR/codequality/idea`.

IntelliJ users: If your IDE doesn't support import of xml files
(versions prior to 2016.1) you can copy manually into
`$HOME/.IntelliJIdea{version}/codestyles`

The complete code style can be found at [Spring Framework Wiki][] but
here's a quick summary:

### Mind the whitespace

Please carefully follow the whitespace and formatting conventions already
present in the framework.

1. Tabs, not spaces
1. Unix (LF), not DOS (CRLF) line endings
1. Eliminate all trailing whitespace
1. Wrap Javadoc at 90 characters
1. Aim to wrap code at 90 characters, but favor readability over wrapping
1. Preserve existing formatting; i.e. do not reformat code for its own sake
1. Search the codebase using `git grep` and other tools to discover common
    naming conventions, etc.
1. Latin-1 (ISO-8859-1) encoding for Java sources; use `native2ascii` to convert
    if necessary


### Add Apache license header to all new classes

```java
/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ...;
```

### Update Apache license header in modified files as necessary

Always check the date range in the license header. For example, if you've
modified a file in 2016 whose header still reads:

```java
/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
```

Then be sure to update it to 2016 accordingly:

```java
/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
```

### Use @since tags for newly-added public API types and methods

For example:

```java
/**
 * ...
 *
 * @author First Last
 * @since 3.0.0
 * @see ...
 */
```

## Prepare Your Commit

### Submit JUnit test cases for all behavior changes

If you know how, test your changes, it will be a great help for all maintainers and will speed up your pull request to be merged.

### Squashing commits

Prefer squashing the commits in your PR in a final single well-commented commit. We
also have the option of "squash-and-merge" the PR at the end though.

Use `git rebase --interactive --autosquash`, `git add --patch`, and other tools
to "squash" multiple commits into a single atomic commit. In addition to the man
pages for git, there are many resources online to help you understand how these
tools work. The [Rewriting History section of Pro Git][] provides a good overview.


### Use real name in git commits

Please configure git to use your real first and last name for any commits you
intend to submit as pull requests. For example, this is not acceptable:

    Author: Nickname <user@mail.com>

Rather, please include your first and last name, properly capitalized, as
submitted against the Spring Individual Contributor License Agreement (ICLA):

    Author: First Last <user@mail.com>

This helps ensure traceability against the ICLA and also goes a long way to
ensuring useful output from tools like `git shortlog` and others.

You can configure this via the account admin area in GitHub (useful for
fork-and-edit cases); _globally_ on your machine with

    git config --global user.name "First Last"
    git config --global user.email user@mail.com

or _locally_ for the `reactor-core` repository only by omitting the
'--global' flag:

    cd reactor-core
    git config user.name "First Last"
    git config user.email user@mail.com


### Format commit messages

Please read and follow the [Commit Guidelines section of Pro Git][].

Most importantly, please format your commit messages in the following way
(adapted from the commit template in the link above):

    Fix #ISSUE Short (50 chars or less) summary of changes

    More detailed explanatory text, if necessary. Wrap it to about 72
    characters or so. In some contexts, the first line is treated as the
    subject of an email and the rest of the text as the body. The blank
    line separating the summary from the body is critical (unless you omit
    the body entirely); tools like rebase can get confused if you run the
    two together.

    Further paragraphs come after blank lines.

     - Bullet points are okay, too

     - Typically a hyphen or asterisk is used for the bullet, preceded by a
       single space, with blank lines in between, but conventions vary here

    Related Issues: gh-1234, gh-1235


1. Use imperative statements in the subject line, e.g. "Fix broken Javadoc link".
1. Begin the subject line with a capitalized verb, e.g. "Add, Prune, Fix,
    Introduce, Avoid, etc."
1. Prefix the subject line with "fix #XXX " if the commit is fixing issue XXX (or
    replace "fix" with "see" if it is only related to an issue without closing it)
1. Do not end the subject line with a period.
1. Restrict the subject line to 72 characters or less.
1. Wrap lines in the body at 72 characters or less.
1. In the body of the commit message, explain how things worked before this
    commit, what has changed, and how things work now.



## Run the Final Checklist

### Run all tests prior to submission

All unit tests can be run using your IDE, if you wish, you can use gradle to do
the job by simply using the `./gradlew test` command

### Submit your pull request

Subject line:

Follow the same conventions for pull request subject lines as mentioned above
for commit message subject lines.

In the body:

1. Explain your use case. What led you to submit this change? Why were existing
   mechanisms in the framework insufficient? Make a case that this is a
   general-purpose problem and that yours is a general-purpose solution, etc.
1. Add any additional information and ask questions; start a conversation or
   continue one from JIRA.
1. Mention the Github issue ID.
1. Also mention that you have submitted the ICLA as described above.

Note that for pull requests containing a single commit, GitHub will default the
subject line and body of the pull request to match the subject line and body of
the commit message. This is fine, but please also include the items above in the
body of the request.


[help documentation]: http://help.github.com/send-pull-requests
[Issues]: https://github.com/reactor/reactor-core/issues
[Spring CLA]: https://cla.pivotal.io/sign/spring
[fork-and-edit]: https://github.com/blog/844-forking-with-the-edit-button
[Spring Framework Wiki]: https://github.com/spring-projects/spring-framework/wiki/Spring-Framework-Code-Style
[Rewriting History section of Pro Git]: http://git-scm.com/book/en/Git-Tools-Rewriting-History
[Commit Guidelines section of Pro Git]: http://git-scm.com/book/en/Distributed-Git-Contributing-to-a-Project#Commit-Guidelines
