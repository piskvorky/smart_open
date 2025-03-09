> Please **pick a concise, informative and complete title** for your PR.
> 
> The title is important because it will appear in [our change log](https://github.com/RaRe-Technologies/smart_open/blob/master/CHANGELOG.md).

### Motivation

> Please explain the motivation behind this PR.
> 
> If you're fixing a bug, link to the issue using a [supported keyword](https://docs.github.com/en/issues/tracking-your-work-with-issues/using-issues/linking-a-pull-request-to-an-issue) like "Fixes #{issue_number}".
> 
> If you're adding a new feature, then consider opening a ticket and discussing it with the maintainers before you actually do the hard work.

Fixes #{issue_number}

### Tests

> If you're fixing a bug, consider [test-driven development](https://en.wikipedia.org/wiki/Test-driven_development):
> 
> 1. Create a unit test that demonstrates the bug. The test should **fail**.
> 2. Implement your bug fix.
> 3. The test you created should now **pass**.
> 
> If you're implementing a new feature, include unit tests for it.
> 
> Make sure all existing unit tests pass.
> You can run them locally using:
> 
>     pytest smart_open
> 
> If there are any failures, please fix them before creating the PR (or mark it as WIP, see below).

### Work in progress

> If you're still working on your PR, mark the PR as [draft PR](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/changing-the-stage-of-a-pull-request).
> 
> We'll skip reviewing it for the time being.
> 
> Once it's ready, mark the PR as "ready for review", and ping one of the maintainers (e.g. mpenkov).

### Checklist

> Before you mark the PR as "ready for review", please make sure you have:

- [ ] Picked a concise, informative and complete title
- [ ] Clearly explained the motivation behind the PR
- [ ] Linked to any existing issues that your PR will be solving
- [ ] Included tests for any new functionality
- [ ] Run `python update_helptext.py` in case there are API changes
- [ ] Checked that all unit tests pass

### Workflow

> Please avoid rebasing and force-pushing to the branch of the PR once a review is in progress.
> 
> Rebasing can make your commits look a bit cleaner, but it also makes life more difficult from the reviewer, because they are no longer able to distinguish between code that has already been reviewed, and unreviewed code.
