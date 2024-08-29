Fixes: *<insert link to GitHub issue here>*

### What was the problem/requirement? (What/Why)

### What was the solution? (How)

### What is the impact of this change?

### How was this change tested?

See [DEVELOPMENT.md](https://github.com/aws-deadline/deadline-cloud/blob/mainline/DEVELOPMENT.md#testing) for information on running tests.

- Have you run the unit tests?
- Have you run the integration tests?
- Have you made changes to the `download` or `asset_sync` modules? If so, then it is highly recommended
  that you ensure that the docker-based unit tests pass.

### Was this change documented?

- Are relevant docstrings in the code base updated?
- Has the README.md been updated? If you modified CLI arguments, for instance.

### Is this a breaking change?

A breaking change is one that modifies a public contract in a way that is not backwards compatible. See the 
[Public Contracts](https://github.com/aws-deadline/deadline-cloud/blob/mainline/DEVELOPMENT.md#public-contracts) section
of the DEVELOPMENT.md for more information on the public contracts.

If so, then please describe the changes that users of this package must make to update their scripts, or Python applications.

### Does this change impact security?

- Does the change need to be threat modeled? For example, does it create or modify files/directories that must only be readable by the process owner?
    - If so, then please label this pull request with the "security" label. We'll work with you to analyze the threats.

----

*By submitting this pull request, I confirm that you can use, modify, copy, and redistribute this contribution, under the terms of your choice.*