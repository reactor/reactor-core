# Welcome to Reactor's Contributor guide!

### Understand the basics

Not sure what a pull request is, or how to submit one? Take a look at GitHub's
excellent [help documentation][https://help.github.com/categories/collaborating-with-issues-and-pull-requests/]
first.

### Search Stack Overflow first; discuss if necessary

If you're unsure why something isn't working or wondering if there is a better
way of doing it please check on Stack Overflow first and if necessary start
a discussion. Use the [`project-reactor`](https://stackoverflow.com/questions/tagged/project-reactor)
tag for that purpose.

### Search Github Issues; create an issue if necessary

Is there already an issue that addresses your concern? Do a bit of searching
in our [Issues](https://github.com/reactor/reactor-core/issues/) to see if you
can find something similar. If you do not find something similar, please create
a new issue before submitting a pull request unless the change is truly trivial
-- for example: typo fixes, removing compiler warnings, etc.

### Sign the Individual Contributor License Agreement (ICLA)

If you have not previously done so, please sign the [Spring CLA form](https://cla.pivotal.io/sign/spring).

It integrates with Github, just follow the steps. Actually, when you first submit a
Pull Request a bot will comment on your PR if you haven't signed the CLA (and if you have,
you'll get a nice green checkmark to the PR checks).

## Wait for triage, then create a Branch

### Triage, Choosing the right starting branch for the PR
Issues are useful before opening a pull-request because they allow the Reactor
team to do some triage, answer questions and help with designing a fix.

Additionally, during the triage process the team will evaluate the criticality
of the reported issue and decide wether or not it should also be fixed in current
maintenance branche(s).

If this is the case, the fix should be implemented against the earliest relevant
maintenance branch, which will be indicated in the issue as the _milestone_.

For instance, if an issue should be fixed in both 3.1 and 3.2, the later being
developed on master, the fix PR should be opened against `3.1.x` (and the issue
will be triaged into `3.1.x Maintenance Backlog`).

If on the other hand the issue should only be fixed in the next feature release,
it will be affected to the `Backlog` triage milestone and should be developed
against `master`.

### Create a branch with representative name

Branches used when submitting pull requests should preferably be named
according to the `xxx-shortDescription` convention:

 - `xxx` is the github issue number
 - use a `-` separator, at least after issue number
 - `shortDescription` should be a few representative words about the issue
 
For example: `1226-lastOnEmptyCallable`.

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
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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
modified a file in 2018 whose header still reads:

```java
/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
 */
```

Then be sure to update it to 2018 accordingly:

```java
/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 */
```

## Prepare Your Commit

### Submit JUnit test cases for all behavior changes

If you know how, test your changes, it will be a great help for all maintainers and will speed up your pull request to be merged.

### Squashing commits

**Once the PR has been approved**, prefer squashing the commits in your PR in a
final single well-commented commit. We also have the option of "squash-and-merge"
the PR at the end though, and that is the default we'll use. 

If your PR is logically split into a few meaningful commits, perform 
a `git rebase -i` instead, to manually ensure the commits are all well formed.

To learn more about these techniques, like `git rebase --interactive --autosquash`,
`git add --patch`, and other tools to "squash" multiple commits into a single
atomic commit, in addition to the man pages for git.
The [Rewriting History section of Pro Git](https://git-scm.com/book/en/v2/Git-Tools-Rewriting-History)
provides a good overview about these tools.


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

We use the following convention for commit message title and body:

```
PREFIX DESCRIPTION

Longer description of the content of the commit and/or context of the
changed, hard-wrapped at 72 characters
```

 - `PREFIX` is either a `[TAG]` or a more conventional `fix #ISSUE`:
    - `[TAG]` is for smaller commits that don't have an issue. We notably use `[test]` for test amendments, `[doc]` for small docs changes like typos, `[build]` for fixes to the CI configuration.
    - most of the time, there should be an issue referenced instead. We prefix with the keyword `fix` so that GitHub will close the issue once the PR is merged. If the PR is a preparation or a follow-up, use `see #ISSUE` instead (eg. `fix #1226` vs `see #1226`).
    
 - `DESCRIPTION` should be a short but meaningful description of the fix, preferably under 50 chars (the whole title line should NOT go over 72 chars)


Here is a more detailed example:

```
    fix #ISSUE Prefer short (50 chars) summary of changes

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

    Related Issues: #1234, gh-1235
```


1. Use imperative statements in the subject line, e.g. "Fix broken Javadoc link".
1. Begin the DESCRIPTION with a capitalized verb, e.g. "Add, Prune, Fix,
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
   continue one from the original GitHub issue.
1. Mention the Github issue ID if there is one.

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
