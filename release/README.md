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
- wget (for summarize_pr.sh)
- jq (for summarize_pr.sh)

All of the above are generally freely available, e.g. installable via apt in Ubuntu.

You'll also need your PyPI username and password (for push_pypi.sh).

## Release Procedure

First, check that the [latest commit](https://github.com/RaRe-Technologies/smart_open/commits/master) passed all CI.

Prepare the release, replacing 1.2.3 with the actual version of the new release:

    bash prepare.sh 1.2.3

This will create a local release branch.
Look around the branch and make sure everything is in order.
Checklist:

- [ ] Does smart_open/VERSION contain the correct version number for the release?
- [ ] Does the CHANGELOG.md contain a section detailing the new release?
- [ ] Are there any PRs that should be in CHANGELOG.md, but currently aren't?

If anything is out of order, make the appropriate changes and commit them to the release branch before proceeding.
For example, you may use the summarize_pr.sh helper script to generate one-line summaries of PRs and copy-paste them into the CHANGELOG.md.

**Once you're happy with the release branch**, run:

    bash merge.sh

This will perform a merge and push your changes to github.com.

**This is the point of no return**.  Run:

    bash push_pypi.sh

and provide your PyPI username and password.

Go to the [releases page](https://github.com/RaRe-Technologies/smart_open/releases/tag) and copy-paste the relevant part of the CHANGELOG.md to the release notes.
Publish the release.

Congratulations, at this stage, you are done!

## Troubleshooting

Ideally, our CI should save you from major boo-boos along the way.
If the build is broken, fix it before even thinking about doing a release.

If anything is wrong with the local release branch (before you call merge.sh), for example:

- Typo in CHANGELOG.md
- Missing entries in CHANGELOG.md
- Wrong VERSION number

then just fix it in the release branch before moving on.

If you've realized there's a problem _after_ calling merge.sh, but _before_ calling push_pypi.sh, then you've made a bit of a mess, but it's still possible to clean it up:

1. Delete the tag for the new version from github.com
2. Delete the tag locally (git tag -d 1.2.3)
3. Repeat from the top using the same version number, and try to get it right this time
