import argparse
import os
import multiprocessing
from multiprocessing import Pool
import logging as log
import shutil
import subprocess

from github import Github, Repository
from tqdm import tqdm


GITHUB_TOKEN_ENV_VAR = "GITHUB_TOKEN"
CSV_NAME = "repositories.csv"
N_CORES = 1


def init_github(login: str = None, password: str = None, token: str = None,
                token_env: str = GITHUB_TOKEN_ENV_VAR) -> Github:
    """
    Initialize entrypoint to access Github API v3.

    :param login: user/login.
    :param password: password.
    :param token: GitHub token.
    :param token_env: Environment variable for GitHub token.
    :return:
    """
    if login is not None and password is not None:
        return Github(login_or_token=login, password=password)
    elif token is not None:
        return Github(login_or_token=token)
    elif os.getenv(token_env, None) is not None:
        return Github(login_or_token=os.getenv(token_env))
    return Github()


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


def clone_repo_multiprocessing(kwargs):
    return clone_repo(**kwargs), kwargs["repo_url"]


def make_repo_dest_dir(repository: Repository.Repository, root_dir: str) -> str:
    """
    Prepare name of destination directory for given repository.

    :param repository: repository.
    :param root_dir: path to root directory to store results.
    :return: output directory location.
    """
    return os.path.join(root_dir, repository.full_name)


def main(args):
    entrypoint = init_github(login=args.login, password=args.password, token=args.token,
                             token_env=args.token_env)
    org = entrypoint.get_organization(args.organization)
    # this step may take a while to complete for big organizations with many repositories
    log.info("Retrieving list ")
    repositories = list(org.get_repos())

    log.info(f"Number of repositories to process is {len(repositories)}")
    os.makedirs(args.output, exist_ok=True)

    arguments = [{"repo_url": r.git_url, "dest": d, "force": f}
                 for r, d, f in zip(repositories, map(lambda r: make_repo_dest_dir(r, args.output),
                                                      repositories),
                                    [args.force] * len(repositories))]
    n_cores = args.cores if args.cores > 0 else multiprocessing.cpu_count()
    with Pool(n_cores) as p:
        results = []
        for dest_dir, repo_url in tqdm(p.imap_unordered(clone_repo_multiprocessing, arguments),
                                       total=len(repositories), unit="repo", desc="repositories"):
            results.append((dest_dir, repo_url))
    good_res = [(dest_dir, repo_url) for dest_dir, repo_url in results if dest_dir]
    log.info(f"{len(good_res)} repositories out of {len(results)} cloned "
             f"successfully")
    csv_loc = os.path.join(args.output, CSV_NAME)
    with open(csv_loc, "w") as f:
        f.write("URL,directory\n")
        for dest_dir, repo_url in good_res:
            f.write(f"{repo_url},{dest_dir}\n")

    log.info(f"{csv_loc} is written.")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--organization", help="Organization name.")
    parser.add_argument("-o", "--output", help="Path to the directory where to store repositories "
                                               "and CSV with list of repositories.")
    # authentication with login-password
    parser.add_argument("-l", "--login", default=None, help="Login. Should be used with password.")
    parser.add_argument("-p", "--password", default=None,  help="Password.")
    # authentication with token
    parser.add_argument("-t", "--token", default=None,  help="GitHub token.")
    parser.add_argument("--token-env", default=GITHUB_TOKEN_ENV_VAR,
                        help="Environment variable for GitHub token.")
    parser.add_argument("-c", "--cores", default=N_CORES, type=int,
                        help="Number of cores to use. If <= 0 - all cores will be used.")
    parser.add_argument("-f", "--force", action="store_true",
                        help="Force to clone repository.")
    # TODO: add logging level
    log.getLogger().setLevel("DEBUG")
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    main(args)
