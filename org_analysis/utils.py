import argparse
import inspect
import io
import os
import logging as log
import requests
import shutil
from shutil import copyfileobj
import subprocess
import tarfile
from urllib.request import urlopen
import gzip

from github import Github

from org_analysis.defaults import GITHUB_TOKEN_ENV_VAR, HERCULES_EXEC


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
    :return: entrypoint to access Github API v3.
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
    """
    Filter kwargs based on signature of function.

    :param kwargs: kwargs.
    :param func: function.
    :return: filtered kwargs.
    """
    func_param = inspect.signature(func).parameters.keys()
    return {k: v for k, v in kwargs.items() if k in func_param}


def download_hercules(output_dir, hercules_exec=HERCULES_EXEC):
    """
    Download latest hercules release archive and extract it.

    :param output_dir: output directory to store results.
    :return: path to executable.
    """
    hercules_output = os.path.join(output_dir, hercules_exec)
    os.makedirs(output_dir, exist_ok=True)
    url = "https://api.github.com/repos/src-d/hercules/releases/latest"
    response = requests.get(url)
    release_url = [asset["browser_download_url"] for asset in response.json()["assets"]
                   if asset["browser_download_url"].endswith("hercules.linux_amd64.gz")][0]
    buffer = io.BytesIO()
    with urlopen(release_url) as response:
        copyfileobj(response, buffer)
    buffer.seek(0)
    with gzip.GzipFile(fileobj=buffer, mode="rb") as gz:
        res = gz.read()

    with open(hercules_output, "wb") as fout:
        fout.write(res)
    os.chmod(hercules_output, 0o775)
    return hercules_output
