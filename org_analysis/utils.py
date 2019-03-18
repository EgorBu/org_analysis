import argparse
import inspect
import os
import logging as log
import shutil
import subprocess

from github import Github

GITHUB_TOKEN_ENV_VAR = "GITHUB_TOKEN"


class ArgumentDefaultsHelpFormatterNoNone(argparse.ArgumentDefaultsHelpFormatter):
    """
    Pretty formatter of help message for arguments.
    It adds default value to the end if it is not None.
    """
    def _get_help_string(self, action):
        if action.default is None:
            return action.help
        return super()._get_help_string(action)


def init_github(login_or_token: str = None, password: str = None,
                token_env: str = GITHUB_TOKEN_ENV_VAR) -> Github:
    """
    Initialize entrypoint to access Github API v3.

    :param login_or_token: user/login or token.
    :param password: password.
    :param token_env: Environment variable for GitHub token.
    :return:
    """
    if login_or_token is None:
        # Try to load token
        login_or_token = os.getenv(token_env)
    return Github(login_or_token=login_or_token, password=password)


def clone_repo(repo_url: str, dest: str = "", force: bool = True) -> str:
    """
    Clone repository to destination (if it was given).

    :param repo_url: repository URL.
    :param dest: destination directory.
    :param force: force to clone repository even if it's exist already.
    :return (destination location or None in case of errors, repo_url).
    """
    cmd = f"git clone --bare {repo_url} {dest}".split()
    if os.path.isdir(dest):
        if force:
            shutil.rmtree(dest)
        else:
            return dest
    try:
        subprocess.check_call(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        return dest
    except subprocess.CalledProcessError as e:
        err = f"Repository {repo_url} failed with exception {e} at clonning step"
        log.error(err)
        log.error(e.stdout)
        log.error(e.stderr)
        return None


def filter_kwargs(kwargs, func):
    func_param = inspect.signature(func).parameters.keys()
    return {k: v for k, v in kwargs.items() if k in func_param}
