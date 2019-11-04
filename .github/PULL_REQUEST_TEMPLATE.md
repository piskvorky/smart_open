#### Title

Please **pick a concise, informative and complete title** for your PR.
The title is important because it will appear in [our change log](https://github.com/RaRe-Technologies/smart_open/blob/master/CHANGELOG.md).

#### Motivation

Please explain the motivation behind this PR in the description.

If you're fixing a bug, link to the issue number like so:

```
- Fixes #{issue_number}
```

If you're adding a new feature, then consider opening a ticket and discussing it with the maintainers before you actually do the hard work.

#### Tests

If you're fixing a bug, consider [test-driven development](https://en.wikipedia.org/wiki/Test-driven_development):

1. Create a unit test that demonstrates the bug. The test should **fail**.
2. Implement your bug fix.
3. The test you created should now **pass**.

If you're implementing a new feature, include unit tests for it.

Make sure all existing unit tests pass.
You can run them locally using:

    pytest smart_open

If there are any failures, please fix them before creating the PR (or mark it as WIP, see below).

#### Work in progress

If you're still working on your PR, include "WIP" in the title.
We'll skip reviewing it for the time being.
Once you're ready to review, remove the "WIP" from the title, and ping one of the maintainers (e.g. mpenkov).

#### Checklist

Before you create the PR, please make sure you have:

- [ ] Picked a concise, informative and complete title
- [ ] Clearly explained the motivation behind the PR
- [ ] Linked to any existing issues that your PR will be solving
- [ ] Included tests for any new functionality
- [ ] Checked that all unit tests pass
