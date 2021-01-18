# Release Scripts

This subdirectory contains various scripts for making a smart_open release.

## Prerequisites

You need a GNU-like environment to run these scripts.  I perform the releases
using Ubuntu 18.04, but other O/S like MacOS should also work.  The
prerequisites are minimal:

- bash
- git with authentication set up (e.g. via ssh-agent)
- virtualenv
- pip

All of the above are generally freely available, e.g. installable via apt in Ubuntu.

## Release Procedure

First, check that the [latest commit](https://github.com/RaRe-Technologies/smart_open/commits/master) passed all CI.

For the subsequent steps to work, you will need to be in the top-level subdirectory for the repo (e.g. /home/misha/git/smart_open).

Prepare the release, replacing 2.3.4 with the actual version of the new release:

    bash release/prepare.sh 2.3.4

This will create a local release branch.
Look around the branch and make sure everything is in order.
Checklist:

- [ ] Does smart_open/version.py contain the correct version number for the release?
- [ ] Does the CHANGELOG.md contain a section detailing the new release?
- [ ] Are there any PRs that should be in CHANGELOG.md, but currently aren't?

If anything is out of order, make the appropriate changes and commit them to the release branch before proceeding.

**This is the point of no return**.
**Once you're happy with the release branch**, run:

    bash release/merge.sh

Congratulations, at this stage, you are done!

## Troubleshooting

Ideally, our CI should save you from major boo-boos along the way.
If the build is broken, fix it before even thinking about doing a release.

If anything is wrong with the local release branch (before you call merge.sh), for example:

- Typo in CHANGELOG.md
- Missing entries in CHANGELOG.md
- Wrong version.py number

then just fix it in the release branch before moving on.

Otherwise, it's too late to fix anything for the current release.
Make a bugfix release to fix the problem.
