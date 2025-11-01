> <img src="https://github.com/user-attachments/assets/cf8f2fa4-37c1-4e50-9fd8-ab6e3fd705b5" width=40%>
> 
> *New GitHub Release dialog*

## Release Procedure

- Check that the [latest commit](https://github.com/piskvorky/smart_open/commits/develop) on `develop` passed all CI.
- Run `bash release/release.sh` to update `CHANGELOG.md` and then update `master` branch.
- The script opens a new browser tab with [new GitHub Release](https://github.com/piskvorky/smart_open/releases/new?target=master).
    - The new version including a `v` prefix shoulb be pre-filled.
    - Confirm that it reads "Excellent! This tag will be created from the target when you publish this release.".
    - Select target branch `master`.
    - Click "Generate release notes" on the right top.
    - Click "Publish release".
    - The GitHub Release and corresponding git tag gets created on the merge commit on `master`.
    - GitHub Actions [`release.yml`](https://github.com/piskvorky/smart_open/actions/workflows/release.yml) is triggered, and uploads distributions to [PyPI](https://pypi.org/project/smart-open/) and to the new [GitHub Release](https://github.com/piskvorky/smart_open/releases).

## Troubleshooting

In case of CI/CD rot:
- Fix it using a separate PR going into develop.
- The failed release tag is lost, never force push git tags!
- Start the above list from the top and create a new release:
  - either make new bugfix release like `7.4.2 -> 7.4.3`
  - or make a post-release like `7.4.2 -> 7.4.2.post1`
