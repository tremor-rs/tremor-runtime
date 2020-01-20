# Contributing to Tremor
[contributing-to-tremor]: #contributing-to-tremor

Thank you for your interest in contributing to the Tremor project! There are many ways to
contribute, and we appreciate all of them. Here's links to the primary ways to contribute
to the Tremor project as an external contributor:

* [Feature Requests](#feature-requests) Request a new feature
* [Bug Reports](#bug-reports) Submit a bug report
* [The Build System](#the-build-system) Contribute to the build system
* [Pull Requests](#pull-requests) Submit a pull request
* [Writing Documentation](#writing-documentation) Contribute content or documentation
* [Issue Triage](#issue-triage) Triage or fix an open issue
* [Out-of-tree Contributions](#out-of-tree-contributions)

If you have questions, please make a post on [internals.tremo.rs][internals] or
hop on to [Tremor Chat][tremor-chat].

As a reminder, all contributors are expected to follow our [Code of Conduct][code-of-conduct].

If this is your first time contributing, we would like to thank you for spending time
on the project! Please reach out directly to any core project member if you would like
any guidance or assistance.

[internals]: https://internals.tremo.rs
[tremor-chat]: http://chat.tremo.rs/
[code-of-conduct]: https://www.tremo.rs/conduct.html

## Feature Requests
[feature-requests]: #feature-requests

To request a change to the way that Tremor works, please head over
to the [RFCs repository](https://github.com/wayfair-incubator/tremor-rfcs) and view the
[README](https://github.com/wayfair-incubator/tremor-rfcs/blob/master/README.md)
for instructions.

## Bug Reports
[bug-reports]: #bug-reports

While bugs are unfortunate, they're a reality in software. We can't fix what we
don't know about, so please report liberally. If you're not sure if something
is a bug or not, feel free to file a bug anyway.

**If you believe reporting your bug publicly represents a security risk to Rust users,
please follow our [instructions for reporting security vulnerabilities](https://www.tremo.rs/policies/security)**.

If you have the chance, before reporting a bug, please [search existing
issues](https://github.com/wayfair-incubator/tremor-runtime/search?q=&type=Issues&utf8=%E2%9C%93),
as it's possible that someone else has already reported your error. This doesn't
always work, and sometimes it's hard to know what to search for, so consider this
extra credit. We won't mind if you accidentally file a duplicate report.

Similarly, to help others who encountered the bug find your issue,
consider filing an issue with a descriptive title, which contains information that might be unique to it.
This can be the language or compiler feature used, the conditions that trigger the bug,
or part of the error message if there is any.
An example could be: **"impossible case reached" on lifetime inference for impl Trait in return position**.

Opening an issue is as easy as following [this
link](https://github.com/wayfair-incubator/tremor-runtime/issues/new) and filling out the fields.
Here's a template that you can use to file a bug, though it's not necessary to
use it exactly:

```
    <short summary of the bug>

    I tried this code:

    <code sample that causes the bug>

    I expected to see this happen: <explanation>

    Instead, this happened: <explanation>

    ## Meta

    `rustc --version --verbose`:

    Backtrace:
```

All three components are important: what you did, what you expected, what
happened instead. Please include the output of `rustc --version --verbose`,
which includes important information about what platform you're on, what
version of Rust you're using, etc.

Sometimes, a backtrace is helpful, and so including that is nice. To get
a backtrace, set the `RUST_BACKTRACE` environment variable to a value
other than `0`. The easiest way
to do this is to invoke `rustc` like this:

```bash
$ RUST_BACKTRACE=1 tremor-server...
```

## The Build System

For info on how to configure and build the project, please see [the tremor build guide][tremor-build-guide]. 
This guide contains info for contributions to the project and the standard facilities. It also lists some
really useful commands to the build system, which could save you a lot of time.

[tremor-build-guide]: https://github.io/wayfair-incubator/tremor-runtime/blob/master/development/how-to-build-and-run.html

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

Please make sure your pull request is in compliance with Tremor's style
guidelines by running

    $ sh ./contrib/pre-commit

Make this check before every pull request (and every new commit in a pull
request); you can add [git hooks](https://git-scm.com/book/en/v2/Customizing-Git-Git-Hooks)
before every push to make sure you never forget to make this check.

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

Speaking of tests, Rust has a comprehensive test suite. More information about
it can be found [here][https://github.com/wayfair-incubator/tremor-runtime/blob/master/docs/development/testing.md].

### External Dependencies

Currently building the Tremor project will also build the following external projects:

* [clippy](https://github.com/rust-lang/rust-clippy)
* [rustfmt](https://github.com/rust-lang/rustfmt)

Breakage is not allowed in released branches and must be addressed before a PR is merged.

## Writing Documentation

Documentation improvements are very welcome. The source of `docs.tremo.rs`
is located in `docs` in the tree. Documentation pull requests function in the same way
as other pull requests.

To find documentation-related issues, sort by the [doc label][tremor-doc-label].

[tremor-doc-label]: https://github.com/wayfair-incubator/tremor-runtime/issues?q=is%3Aopen%20is%3Aissue%20label%3Adoc

You can find documentation style guidelines in [RFC 1574][rfc1574].

[rfc1574]: https://github.com/rust-lang/rfcs/blob/master/text/1574-more-api-documentation-conventions.md#appendix-a-full-conventions-text

Additionally, contributions to the [tremor-guide] are always welcome. Contributions
can be made directly [here](https://github.com/wayfair-incubator/tremor-runtime) repo. The issue
tracker in that repo is also a great way to find things that need doing.

## Issue Triage

Sometimes, an issue will stay open, even though the bug has been fixed. And
sometimes, the original bug may go stale because something has changed in the
meantime.

It can be helpful to go through older bug reports and make sure that they are
still valid. Load up an older issue, double check that it's still true, and
leave a comment letting us know if it is or is not. The [least recently
updated sort][lru] is good for finding issues like this.

[lru]: https://github.com/wayfair-incubator/tremor-runtime/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-asc

## Out-of-tree Contributions

There are a number of other ways to contribute to Tremor that don't deal with
this repository.

Answer questions in the _Get Help!_ channels from the [Tremor Chat][tremor-chat].

Participate in the [RFC process](https://github.com/wayfair-incubator/tremor-rfcs).

