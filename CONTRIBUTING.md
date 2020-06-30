# Contributing to Tremor

[contributing-to-tremor]: #contributing-to-tremor

Thank you for your interest in contributing to the Tremor project! There are many ways to
contribute, and we appreciate all of them. Here's links to the primary ways to contribute
to the Tremor project as an external contributor:

- [Contributing to Tremor](#contributing-to-tremor)
  - [Feature Requests](#feature-requests)
  - [Bug Reports](#bug-reports)
  - [The Build System](#the-build-system)
  - [Pull Requests](#pull-requests)
    - [External Dependencies](#external-dependencies)
  - [Writing Documentation](#writing-documentation)
  - [Issue Triage](#issue-triage)
  - [Out-of-tree Contributions](#out-of-tree-contributions)
  - [Tremor Chat](#tremor-chat)

If you have questions, please make a query hop on over to [Tremor Chat][tremor-chat].

As a reminder, all contributors are expected to follow our [Code of Conduct][code-of-conduct].

If this is your first time contributing, we would like to thank you for spending time
on the project! Please reach out directly to any core project member if you would like
any guidance or assistance.

[code-of-conduct]: https://docs.tremor.rs/CodeOfConduct

## Feature Requests

[feature-requests]: #feature-requests

To request a change to the way that Tremor works, please head over
to the [RFCs repository](https://github.com/wayfair-tremor/tremor-rfcs) and view the
[README](https://github.com/wayfair-tremor/tremor-rfcs/blob/master/README.md)
for instructions.

## Bug Reports

[bug-reports]: #bug-reports

While bugs are unfortunate, they're a reality in software. We can't fix what we
don't know about, so please report liberally. If you're not sure if something
is a bug or not, feel free to file a bug anyway.

**If you believe reporting your bug publicly represents a security risk to Tremor users,
please follow our [instructions for reporting security vulnerabilities](https://docs.tremor.rs/policies/security)**.

If you have the chance, before reporting a bug, please [search existing
issues](https://github.com/wayfair-tremor/tremor-runtime/search?q=&type=Issues&utf8=%E2%9C%93),
as it's possible that someone else has already reported your error. This doesn't
always work, and sometimes it's hard to know what to search for, so consider this
extra credit. We won't mind if you accidentally file a duplicate report.

Similarly, to help others who encountered the bug find your issue,
consider filing an issue with a descriptive title, which contains information that might be unique to it.
This can be the language or compiler feature used, the conditions that trigger the bug,
or part of the error message if there is any.
An example could be: **"impossible case reached" on match expression in tremor scripting language**.

Opening an issue only requires following [this
link](https://github.com/wayfair-tremor/tremor-runtime/issues/new) and filling out the fields.
Here's a template that you can use to file a bug, though it's not necessary to
use it exactly:

```
    <short summary of the bug>

    I tried this code:

    <code sample that causes the bug>

    I expected to see this happen: <explanation>

    Instead, this happened: <explanation>

    ## Meta

    `tremor-script --version`:

    Backtrace:
```

All three components are important: what you did, what you expected, what
happened instead. Please include the output of `tremor-server --version`,
which includes important information about what platform you're on, what
version of Rust you're using, etc.

Sometimes, a backtrace is helpful, and so including that is nice. To get
a backtrace, set the `RUST_BACKTRACE` environment variable to a value
other than `0`. The easiest way to do this is to invoke `tremor-server` like this:

```bash
$ RUST_BACKTRACE=1 tremor-server...
```

## The Build System

For info on how to configure and build the project, please see [the tremor build guide][tremor-build-guide].
This guide contains info for contributions to the project and the standard facilities. It also lists some
really useful commands to the build system, which could save you a lot of time.

[tremor-build-guide]: http://docs.tremor.rs/development/quick-start/

## Pull Requests

[pull-requests]: #pull-requests

Pull requests are the primary mechanism we use to change Tremor. GitHub itself
has some [great documentation][about-pull-requests] on using the Pull Request feature.
We use the "fork and pull" model [described here][development-models], where
contributors push changes to their personal fork and create pull requests to
bring those changes into the source repository.

[about-pull-requests]: https://help.github.com/articles/about-pull-requests/
[development-models]: https://help.github.com/articles/about-collaborative-development-models/

Please make pull requests against the `master` branch.

Tremor follows a no merge policy, meaning, when you encounter merge
conflicts you are expected to always rebase instead of merge.
E.g. always use rebase when bringing the latest changes from
the master branch to your feature branch.
Also, please make sure that fixup commits are squashed into other related
commits with meaningful commit messages.

GitHub allows [closing issues using keywords][closing-keywords]. This feature
should be used to keep the issue tracker tidy. However, it is generally preferred
to put the "closes #123" text in the PR description rather than the issue commit;
particularly during rebasing, citing the issue number in the commit can "spam"
the issue in question.

[closing-keywords]: https://help.github.com/en/articles/closing-issues-using-keywords

All pull requests are reviewed by another person.

If you want to request that a specific person reviews your pull request,
you can add an `r?` to the pull request description. For example, [Darach Ennis][darach] usually reviews
documentation changes. So if you were to make a documentation change, add

    r? @darach

to the end of the pull request description. This is entirely optional.

After someone has reviewed your pull request, they will leave an annotation
on the pull request with an `r+`. It will look something like this:

    r+

Once your merge request is approved it will enter the merge queue

[darach]: https://github.com/darach

Speaking of tests, tremor has a comprehensive test suite. More information about
it can be found [here][https://github.com/wayfair-tremor/tremor-www-docs/blob/master/docs/development/testing.md].

### External Dependencies

Currently building the Tremor project will also build the following external projects:

- [clippy](https://github.com/rust-lang/rust-clippy)
- [rustfmt](https://github.com/rust-lang/rustfmt)

Breakage is not allowed in released branches and must be addressed before a PR is merged.

## Writing Documentation

Documentation improvements are very welcome. The source of `docs.tremor.rs`
is located in the [tremor docs repo](https://github.com/wayfair-tremor/tremor-www-docs). Documentation pull requests function in the same way as other pull requests.

To find documentation-related issues, sort by the [doc label][tremor-doc-label].

[tremor-doc-label]: https://github.com/wayfair-tremor/tremor-www-docs/issues?q=is%3Aopen%20is%3Aissue%20label%3Adoc

Additionally, contributions to the [tremor-guide] are always welcome. Contributions
can be made directly [here](https://github.com/wayfair-tremor/tremor-www-docs) repo. The issue
tracker in that repo is also a great way to find things that need doing.

## Issue Triage

Sometimes, an issue will stay open, even though the bug has been fixed. And
sometimes, the original bug may go stale because something has changed in the
meantime.

It can be helpful to go through older bug reports and make sure that they are
still valid. Load up an older issue, double check that it's still true, and
leave a comment letting us know if it is or is not. The [least recently
updated sort][lru] is good for finding issues like this.

[lru]: https://github.com/wayfair-tremor/tremor-runtime/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-asc

## Out-of-tree Contributions

There are a number of other ways to contribute to Tremor that don't deal with
this repository.

Answer questions in the _Get Help!_ channels from the [Tremor Chat][tremor-chat].

Participate in the [RFC process](https://github.com/wayfair-tremor/tremor-rfcs).

## Tremor Chat

[tremor-chat]: #tremor-chat

Join the tremor community [slack](https://join.slack.com/t/tremor-debs/shared_invite/enQtOTMxNzY3NDg0MjI2LTQ4MTU4NjlkZDk0MmJmNmIwYjU0ZDc1OTNjMGRmNzUwZTdlZGVkMWFmNGFkZTAwOWJlYjlkMDZkNGNiMjQ2NzI)
