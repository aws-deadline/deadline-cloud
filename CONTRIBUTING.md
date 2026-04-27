# Contributing Guidelines

Thank you for your interest in contributing to the AWS Deadline Cloud
Rust client. Whether it's a bug report, new feature, correction, or
additional documentation, we greatly value feedback and contributions
from our community.

Please read through this document before submitting any issues or pull
requests to ensure we have all the necessary information to effectively
respond to your bug report or contribution.

## Table of Contents

- [Reporting Bugs/Feature Requests](#reporting-bugsfeature-requests)
- [Development](#development)
  - [Finding contributions to work on](#finding-contributions-to-work-on)
  - [Talk with us first](#talk-with-us-first)
  - [Contributing via Pull Requests](#contributing-via-pull-requests)
  - [Conventional Commits](#conventional-commits)
- [Licensing](#licensing)

## Reporting Bugs/Feature Requests

We welcome you to use the GitHub issue tracker to report bugs or suggest
features.

When filing an issue, please check existing open, or recently closed,
issues to make sure somebody else hasn't already reported the issue.
Please try to include as much information as you can.

## Development

We welcome you to contribute features and bug fixes via a
[pull request](https://help.github.com/articles/creating-a-pull-request/).
If you are new to contributing to GitHub repositories, then you may find
the [GitHub documentation on collaborating with the fork and pull model](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/getting-started/about-collaborative-development-models#fork-and-pull-model)
informative; this is the model that we follow.

Please see [DEVELOPMENT.md](./DEVELOPMENT.md) for information about how
to navigate this repository's code base and development practices.

### Finding contributions to work on

If you are not sure what you would like to contribute, then looking at
the existing issues is a great way to find something to contribute on.
Looking at issues that have the "help wanted" or "good first issue"
labels are a good place to start, but please dive into any issue that
interests you whether it has those labels or not.

### Talk with us first

We ask that you please open a feature request issue (if one does not
already exist) and talk with us before posting a pull request that
contains a significant amount of work, or one that proposes a change to
a public interface. We want to make sure that your time and effort is
respected by working with you to design the change before you spend much
of your time on it. If you want to create a draft pull request to show
what you are thinking and then talk with us, then that works with us as
well.

### Contributing via Pull Requests

Contributions via pull requests are much appreciated. Before sending us
a pull request, please ensure that:

1. You are working against the latest source on the *mainline* branch.
2. You check existing open, and recently merged, pull requests to make
   sure someone else hasn't addressed the problem already.
3. You open an issue to discuss any significant work — we would hate for
   your time to be wasted.
4. Your pull request will be focused on a single change — it is easier
   for us to understand when a change is focused rather than changing
   multiple things at once.

To send us a pull request, please:

1. Fork the repository.
2. Modify the source and add tests for your change; please focus on the
   specific change you are contributing. If you also reformat all the
   code, it will be hard for us to focus on your change. Please see
   [DEVELOPMENT.md](./DEVELOPMENT.md) for tips.
3. Ensure tests pass. Please see the
   [Testing](./DEVELOPMENT.md#testing) section for information on tests.
4. Commit to your fork using clear commit messages. Note that this
   repository requires the use of
   [conventional commit](#conventional-commits) syntax for the title of
   your commit.
5. Send us a pull request, answering any default questions in the pull
   request interface.
6. Pay attention to any automated CI failures reported in the pull
   request, and stay involved in the conversation.

GitHub provides additional documentation on
[forking a repository](https://help.github.com/articles/fork-a-repo/)
and
[creating a pull request](https://help.github.com/articles/creating-a-pull-request/).

### Conventional Commits

The commits in this repository are all required to use
[conventional commit syntax](https://www.conventionalcommits.org/en/v1.0.0/)
in their title to help us identify the kind of change that is being
made, automatically generate the changelog, and automatically identify
next release version number. Only the first commit that deviates from
mainline in your pull request must adhere to this requirement.

We ask that you use these commit types in your commit titles:

- `feat` — When the pull request adds a new feature or functionality
- `fix` — When the pull request is implementing a fix to a bug
- `test` — When the pull request is only implementing an addition or
  change to tests or the testing infrastructure
- `docs` — When the pull request is primarily implementing an addition
  or change to the package's documentation
- `refactor` — When the pull request is implementing only a refactor of
  existing code
- `ci` — When the pull request is implementing a change to the CI
  infrastructure of the package
- `chore` — When the pull request is a generic maintenance task
- `perf` — When the pull request is a performance improvement

We also require that the type in your conventional commit title end in
an exclamation point (e.g. `feat!` or `fix!`) if the pull request should
be considered to be a breaking change in some way. Please also include a
"BREAKING CHANGE" footer in the description of your commit in this case
([example](https://www.conventionalcommits.org/en/v1.0.0/#commit-message-with-both--and-breaking-change-footer)).

## Code of Conduct

This project has adopted the
[Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct).
For more information see the
[Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq) or
contact opensource-codeofconduct@amazon.com with any additional questions
or comments.

## Security Issue Notifications

If you discover a potential security issue in this project we ask that
you notify AWS/Amazon Security via our
[vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/).
Please do **not** create a public GitHub issue.

## Licensing

See the [LICENSE](LICENSE) file for our project's licensing. We will ask
you to confirm the licensing of your contribution.
