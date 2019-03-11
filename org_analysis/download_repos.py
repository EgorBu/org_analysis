import argparse
import os
import multiprocessing
from multiprocessing import Pool
import logging as log


from github import UnknownObjectException, Repository
from tqdm import tqdm

from org_analysis.utils import ArgumentDefaultsHelpFormatterNoNone, clone_repo, init_github, \
    GITHUB_TOKEN_ENV_VAR


CSV_NAME = "repositories.csv"
N_CORES = 1


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
    entrypoint = init_github(login_or_token=args.login, password=args.password,
                             token_env=args.token_env)
    try:
        org_or_user = entrypoint.get_organization(args.organization)
    except UnknownObjectException:
        # switch to user
        org_or_user = entrypoint.get_user(args.organization)
    # this step may take a while to complete for big organizations with many repositories
    log.info("Retrieving a list of repositories...")
    repositories = list(org_or_user.get_repos())

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


def add_download_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("-n", "--organization", help="Organization name.", required=True)
    parser.add_argument("-o", "--output", help="Path to the directory where to store repositories "
                                               "and CSV with list of repositories.", required=True)
    # authentication with login-password/token
    parser.add_argument("-l", "--login", default=None, help="Login or token.")
    parser.add_argument("-p", "--password", default=None,  help="Password.")
    parser.add_argument("--token-env", default=GITHUB_TOKEN_ENV_VAR,
                        help="Environment variable for GitHub token.")
    parser.add_argument("-c", "--cores", default=N_CORES, type=int,
                        help="Number of cores to use. If <= 0 - all cores will be used.")
    parser.add_argument("-f", "--force", action="store_true",
                        help="Force to clone repository.")

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatterNoNone)
    parser.add_argument("--log-level", default="INFO", choices=log._nameToLevel,
                        help="Logging verbosity.")
    args = add_download_args(parser)
    # TODO: add logging level
    log.getLogger().setLevel(args.log_level)
    main(args)
