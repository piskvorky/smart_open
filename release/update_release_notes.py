"""Helper script for updating the release notes.

Copies the change log to the window manager clipboard.
Opens the release notes using the browser.
All you have to do is paste and click "commit changes".
"""
import os
import sys
import webbrowser

version = sys.argv[1]
curr_dir = os.path.dirname(__file__)


def copy_to_clipboard(text):
    try:
        import pyperclip
    except ImportError:
        print('pyperclip <https://pypi.org/project/pyperclip/> is missing.', file=sys.stderr)
        print('copy-paste the contents of CHANGELOG.md manually', file=sys.stderr)
    else:
        pyperclip.copy(text)


with open(os.path.join(curr_dir, '../CHANGELOG.md')) as fin:
    copy_to_clipboard(fin.read())


url = "https://github.com/RaRe-Technologies/smart_open/releases/tag/v%s" % version
webbrowser.open(url)
