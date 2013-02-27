from subprocess import Popen, PIPE


def get_git_commit():
    """Return a string that contains the result of the git-describe command. Return
    'unknown' if git-descibe does not exist."""
    try:
        commit = Popen(["git", "describe"], stdout=PIPE).communicate()[0].strip()
        # the following only works in Python 2.7
        # commit = subprocess.check_output(['git', 'describe']).strip()
        return commit
    except OSError:
        return "unknown"
