# Release Scripts

This subdirectory contains various scripts for maintainers.

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

> ![New GitHub Release](https://github.com/user-attachments/assets/cf8f2fa4-37c1-4e50-9fd8-ab6e3fd705b5)
> *New GitHub Release dialog*

- Check that the [latest commit](https://github.com/piskvorky/smart_open/commits/develop) on `develop` passed all CI.
- Draft a [new GitHub Release](https://github.com/piskvorky/smart_open/releases/new).
    - Fill in the new tag including a `v` prefix and press enter.
    - Confirm that it reads "Excellent! This tag will be created from the target when you publish this release.".
    - Select target branch `develop` (for the release notes).
    - Click "Generate release notes" on the right top.
    - Select target branch `master` (for the actual release).
    - Keep the tab open in your browser.
- Make sure you're on `develop` and you're up to date locally:
    - `git checkout develop && git pull`
- Copy the generated bullet points from your browser into `CHANGELOG.md`.
- Commit `CHANGELOG.md` to `develop` and push:
    - `git add CHANGELOG.md && git commit -m "Update CHANGELOG.md" && git push`
- Make sure you're on `master` and you're up to date locally:
    - `git checkout master && git pull`
- Merge `develop` into `master` and push:
    - `git pull origin develop --no-ff --no-edit && git push`
- Click "Publish release" in your browser.
    - The GitHub Release and corresponding git tag gets created on the merge commit on `master`.
    - GitHub Actions [`release.yml`](https://github.com/piskvorky/smart_open/actions/workflows/release.yml) is triggered, and uploads distributions to [PyPI](https://pypi.org/project/smart-open/) and to the new [GitHub Release](https://github.com/piskvorky/smart_open/releases).

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
