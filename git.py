import subprocess


def get_git_commit():

    try:
        commit = subprocess.check_output(['git', 'describe']).strip()
        return commit
    except OSError:
        return "unknown"
