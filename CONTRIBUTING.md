# Contributing Guidelines

Thank you for your interest in contributing to our project. Whether it's a bug report, new feature, correction, or additional
documentation, we greatly value feedback and contributions from our community.

Please read through this document before submitting any issues or pull requests to ensure we have all the necessary
information to effectively respond to your bug report or contribution.

Table of contents:

* [Reporting Bugs/Feature Requests](#reporting-bugsfeature-requests)
* [Development](#development)
    * [Finding contributions to work on](#finding-contributions-to-work-on)
    * [Talk with us first](#talk-with-us-first)
    * [Contributing via Pull Requests](#contributing-via-pull-requests)
    * [Conventional Commits](#conventional-commits)
    * [Digital Certificate of Origin](#digital-certificate-of-origin)
    * [Conventional Commit & DCO Automation](#automating-conventional-commits-and-dco)
* [Licensing](#licensing)

## Reporting Bugs/Feature Requests

We welcome you to use the GitHub issue tracker to report bugs or suggest features.

When filing an issue, please check existing open, or recently closed, issues to make sure somebody else hasn't already
reported the issue. Please try to include as much information as you can.

## Development

We welcome you to contribute features and bug fixes via a [pull request](https://help.github.com/articles/creating-a-pull-request/).
If you are new to contributing to GitHub repositories, then you may find the 
[GitHub documentation on collaborating with the fork and pull model](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/getting-started/about-collaborative-development-models#fork-and-pull-model)
informative; this is the model that we follow.

Please see [DEVELOPMENT.md](./DEVELOPMENT.md) for information about how to navigate this package's
code base and development practices.

### Finding contributions to work on

If you are not sure what you would like to contribute, then looking at the existing issues is a great way to find
something to contribute on. Looking at 
[issues that have the "help wanted" or "good first issue" labels](https://github.com/aws-deadline/deadline-cloud/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22%2C%22help+wanted%22)
are a good place to start, but please dive into any issue that interests you whether it has those labels or not.

### Talk with us first

We ask that you please [open a feature request issue](https://github.com/aws-deadline/deadline-cloud/issues/new/choose)
(if one does not already exist) and talk with us before posting a pull request that contains a significant amount of work,
or one that proposes a change to a public interface such as to the interface of a publicly exported Python function or to
the command-line interfaces' commands or arguments. We want to make sure that your time and effort is respected by working
with you to design the change before you spend much of your time on it. If you want to create a draft pull request to show what
you are thinking and then talk with us, then that works with us as well.

We prefer that this package contain primarily features that are useful to many users of it, rather than features that are specific
to niche workflows. If you have a feature in mind, but are not sure whether it is niche or not then please 
[open a feature request issue](https://github.com/aws-deadline/deadline-cloud/issues/new/choose). We will do our best to help
you make that assessment, and posting a public issue will help others find your feature idea and add their support if they
would also find it useful.

### Contributing via Pull Requests

Contributions via pull requests are much appreciated. Before sending us a pull request, please ensure that:

1. You are working against the latest source on the *mainline* branch.
2. You check existing open, and recently merged, pull requests to make sure someone else hasn't addressed the problem already.
3. You open an issue to discuss any significant work - we would hate for your time to be wasted.
4. Your pull request will be focused on a single change - it is easier for us to understand when a change is focused rather
   than changing multiple things at once.

To send us a pull request, please:

1. Fork the repository.
2. Modify the source and add tests for your change; please focus on the specific change you are contributing.
   If you also reformat all the code, it will be hard for us to focus on your change.
   Please see [DEVELOPMENT.md](./DEVELOPMENT.md) for tips.
3. Ensure tests pass. Please see the [Testing](./DEVELOPMENT.md#testing) section for information on tests.
4. Commit to your fork using clear commit messages. Note that all AWS Deadline Cloud GitHub repositories require the use
   of [conventional commit](#conventional-commits) syntax for the title of your commit.
5. Send us a pull request, answering any default questions in the pull request interface.
6. Pay attention to any automated CI failures reported in the pull request, and stay involved in the conversation.

GitHub provides additional documentation on [forking a repository](https://help.github.com/articles/fork-a-repo/) and
[creating a pull request](https://help.github.com/articles/creating-a-pull-request/).

### Conventional commits

The commits in this repository are all required to use [conventional commit syntax](https://www.conventionalcommits.org/en/v1.0.0/)
in their title to help us identify the kind of change that is being made, automatically generate the changelog, and 
automatically identify next release version number. Only the first commit that deviates from mainline in your pull request
must adhere to this requirement.

We ask that you use these commit types in your commit titles:

* `feat` - When the pull request adds a new feature or functionality;
* `fix` - When the pull request is implementing a fix to a bug;
* `test` - When the pull request is only implementing an addition or change to tests or the testing infrastructure;
* `docs` - When the pull request is primarily implementing an addition or change to the package's documentation;
* `refactor` - When the pull request is implementing only a refactor of existing code;
* `ci` - When the pull request is implementing a change to the CI infrastructure of the packge;
* `chore` - When the pull request is a generic maintenance task.

We also require that the type in your conventional commit title end in an exclaimation point (e.g. `feat!` or `fix!`)
if the pull request should be considered to be a breaking change in some way. Please also include a "BREAKING CHANGE" footer
in the description of your commit in this case ([example](https://www.conventionalcommits.org/en/v1.0.0/#commit-message-with-both--and-breaking-change-footer)).
Examples of breaking changes include any that implements a backwards-imcompatible change to a public Python interface,
the command-line interface, or the like. 

If you need change a commit message, then please see the
[GitHub documentation on the topic](https://docs.github.com/en/pull-requests/committing-changes-to-your-project/creating-and-editing-commits/changing-a-commit-message)
to guide you.

### Digital Certificate of Origin.

This repository enforces the Developer Certificate of Origin (DCO) on Pull Requests. It requires all commit messages to contain the `Signed-off-by` line with an email address that matches the commit author.

To generate the `Signed-off-by` line, make sure `git commit` is passed with the `-s` flag.

### Automating Conventional Commits and DCO.

To simplify Conventional Commit and DCO, it is recommended to setup a Git hook for the repository. Add the following as a git hook:

- Edit `{repository}/.git/hooks/commit-msg`
- `chmod +x {repository}/.git/hooks/commit-msg`
- Paste in the following shell script, customize as necessary.
```
#!/bin/sh
SIGNATURE="Signed-off-by: `git config --global --get user.name` <`git config --global --get user.email`>"
grep -qs "^${SIGNATURE}" "$1" || echo "\n${SIGNATURE}" >> "$1"

commitTitle="$(cat $1 | head -n1)"

# ignore merge requests
if echo "$commitTitle" | grep -qE "Merge branch"; then
	echo "Commit hook: ignoring branch merge"
	exit 0
fi

# check semantic versioning scheme
if ! echo "$commitTitle" | grep -qE '^(feat|fix|docs|style|refactor|perf|test|chore|build|ci|revert)(\([a-z0-9\s\-\_\,]+\))?!?:\s\w'; then
	echo "Your commit message did not follow semantic versioning: $commitTitle"
	echo ""
	echo "Format:   <type>(<optional-scope>): <subject>"
	echo "Example:  feat(api): add endpoint"
	echo ""
	echo "Valid types: build|chore|ci|docs|feat|fix|perf|refactor|revert|style|test"
	echo ""
	echo "Please see"
	echo "- https://www.conventionalcommits.org/en/v1.0.0/#summary"
	exit 1
fi
```

If you want to create this hook automatically on every new git repository you will create from now on (using git init, git clone...) you can use git-templates; to do so, just save the script described above into (duplicate files):

```
~/.git-templates/hooks/commit-msg
```

Configure the Git default as follows:
```
git config --global init.templatedir ~/.git-templates
```

## Licensing

See the [LICENSE](LICENSE) file for our project's licensing. We will ask you to confirm the licensing of your contribution.
