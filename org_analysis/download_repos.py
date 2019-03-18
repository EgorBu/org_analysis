"""Functionality related to retrieving list of repositories and cloning them."""
import argparse
import os
import multiprocessing
from multiprocessing import Pool
import logging as log


from github import UnknownObjectException, Repository
from tqdm import tqdm

from org_analysis.defaults import CSV_NAME, DIRECTORY_FIELD_NAME, GITHUB_TOKEN_ENV_VAR, N_CORES, \
    URL_FIELD_NAME
from org_analysis.utils import ArgumentDefaultsHelpFormatterNoNone, clone_repo, init_github, \
    filter_kwargs


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


def handler(login, password, token_env, organization, cores, output, force, csv_name,
            url_field_name, directory_field_name):
    """
    Retrieve list of repositories in organization/user and download them to output directory and
    save CSV with fields `URL,directory`.

    :param login: login or GitHub token.
    :param password: GitHub password.
    :param token_env: environment variable to store GitHub token.
    :param organization: organization name.
    :param cores: number of cores to use for downloading repositories.
    :param output: output directory.
    :param force: if not force and repository was cloned already - nothing will be done. If force
                  and repository was cloned - repository will be deleted and cloned again.
    :param csv_name: name of csv to store statistics.
    :param url_field_name: name of URL field in CSV (GitHub URL).
    :param directory_field_name: name of directory field in CSV (path to repository)..
    """
    entrypoint = init_github(login_or_token=login, password=password,
                             token_env=token_env)
    try:
        org_or_user = entrypoint.get_organization(organization)
    except UnknownObjectException:
        # switch to user
        org_or_user = entrypoint.get_user(organization)
    # this step may take a while to complete for big organizations with many repositories
    log.info("Retrieving a list of repositories...")
    repositories = list(org_or_user.get_repos())

    log.info(f"Number of repositories to process is {len(repositories)}")
    os.makedirs(output, exist_ok=True)

    arguments = [{"repo_url": r.git_url, "dest": d, "force": f}
                 for r, d, f in zip(repositories, map(lambda r: make_repo_dest_dir(r, output),
                                                      repositories),
                                    [force] * len(repositories))]
    n_cores = cores if cores > 0 else multiprocessing.cpu_count()
    with Pool(n_cores) as p:
        results = []
        for dest_dir, repo_url in tqdm(p.imap_unordered(clone_repo_multiprocessing, arguments),
                                       total=len(repositories), unit="repo", desc="repositories"):
            results.append((dest_dir, repo_url))
    good_res = [(dest_dir, repo_url) for dest_dir, repo_url in results if dest_dir]
    log.info(f"{len(good_res)} repositories out of {len(results)} cloned "
             f"successfully")
    csv_loc = os.path.join(output, csv_name)
    with open(csv_loc, "w") as f:
        f.write(f"{url_field_name},{directory_field_name}\n")
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
    parser.add_argument("--csv-name", default=CSV_NAME, help="Name of csv to store statistics.")
    parser.add_argument("--url-field-name", default=URL_FIELD_NAME,
                        help="Name of URL field in CSV (GitHub URL).")
    parser.add_argument("--directory-field-name", default=DIRECTORY_FIELD_NAME,
                        help="Name of directory field in CSV (it contains path to repository).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatterNoNone)
    parser.add_argument("--log-level", default="INFO", choices=log._nameToLevel,
                        help="Logging verbosity.")
    add_download_args(parser)
    args = parser.parse_args()
    log.getLogger().setLevel(args.log_level)
    download_kwargs = filter_kwargs(vars(args), handler)
    handler(**download_kwargs)
