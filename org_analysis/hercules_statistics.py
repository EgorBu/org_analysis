"""Calculate statistics for multiple repositories and merge them using src-d/hercules."""
import argparse
from datetime import datetime
import multiprocessing
from multiprocessing import Pool
import os
import logging as log
import subprocess
import sys
import tempfile
from time import time
from typing import NamedTuple, Sequence, Tuple

import pandas as pd
import tqdm

from org_analysis.defaults import AGGREGATED_STATISTICS_NAME, DIRECTORY_FIELD_NAME, HERCULES_EXEC,\
    N_CORES, SIZE_LIMIT, URL_FIELD_NAME
from org_analysis.utils import filter_kwargs
from hercules import labours

sys.path.append("hercules")

ReportStat = NamedTuple("ReportStat",
                        (("repo_size", int),
                         ("duration", int),
                         ("err", str),
                         ("repository", str)))


def dir_size(loc: str) -> int:
    """
    Measure size of directory.

    :param loc: location of directory.
    :return: size in bytes.
    """
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(loc):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                total_size += os.path.getsize(fp)
            except:
                pass
    return total_size


def repository_statistics(repo_url: str, repo_loc: str, output_dir: str,
                          hercules_exec: str = HERCULES_EXEC, size_limit: int = SIZE_LIMIT,
                          force: bool = True) -> (ReportStat, str):
    """
    Calculate statistics for given repository and save results.

    :param repo_url: repository URL.
    :param repo_loc: directory with repository.
    :param output_dir: where to create directories for repo and save "statistics.pb".
    :param hercules_exec: location of hercules executable.
    :param size_limit: size limit - skip repository if it's bigger than size_limit.
    :param force: Force overwriting of existing statistic.
    :return: (ReportStat, path).
             ReportStat contains statistics about repository:
                size - size of git in bytes (0 in case if caching step failed)
                duration of processing - in seconds
                repository name
                error logs in case of error or None
            path to file with statistics.
    """
    start = time()
    if not os.path.isdir(repo_loc) or os.path.abspath(repo_loc) != repo_loc:
        raise ValueError(f"Absolute path expected, got {repo_loc}")
    if os.path.abspath(output_dir) != output_dir:
        raise ValueError(f"Absolute path expected, got {output_dir}")

    result_dir = os.path.join(output_dir, *repo_url.split("/")[-2:])
    stat_loc = os.path.join(result_dir, f"statistics.pb")
    repo_size = dir_size(repo_loc)
    if os.path.isfile(stat_loc) and not force:
        log.info(f"{repo_loc}: statistics were calculated already and not required to "
                 f"recalculate - skipping next steps.")
        return (ReportStat(repo_size=repo_size, duration=time() - start, repository=repo_loc,
                           err=""),
                stat_loc)

    if repo_size > size_limit > 0:
        err = f"Repository {repo_loc} is too big: {repo_size} bytes > {size_limit} - skipping"
        log.error(err)
        return (ReportStat(repo_size=repo_size, duration=time() - start, repository=repo_loc,
                           err=err),
                None)

    os.makedirs(result_dir, exist_ok=True)  # create subdirectories for org/name if needed

    # calculate statistics
    cmd = [hercules_exec]
    # use protobuf to merge results for several repositories
    cmd.append(f"--pb")
    # burndown. TODO: check if it's needed
    cmd.extend("--burndown --burndown-people".split())
    # devs analysis
    cmd.append("--devs")
    # couples analysis - for developer-file matching
    cmd.append("--couples")
    # for stability
    cmd.append("--hibernation-distance=1000")
    # exclude vendors
    cmd.append("--skip-blacklist")
    # cache to analyse
    cmd.append(repo_loc)
    try:
        with open(stat_loc, "wb") as f:
            # write results to file
            subprocess.run(cmd, check=True, stdout=f)
    except subprocess.CalledProcessError as e:
        err = f"Repository {repo_loc} failed with exception {e} at step of calculating statistics"
        log.error(err)
        log.error("Fall back to more stable option")
        # for stability
        cmd.append("--first-parent")
        try:
            with open(stat_loc, "wb") as f:
                # write results to file
                subprocess.run(cmd, check=True, stdout=f)
        except subprocess.CalledProcessError as e:
            err = f"Repository {repo_loc} failed with exception {e} at step of calculating " \
                  f"statistics"
            log.error(err)
            return (ReportStat(repo_size=repo_size, duration=time() - start,
                               repository=repo_loc, err=err),
                    None)
    return (ReportStat(repo_size=repo_size, duration=time() - start,
                       repository=repo_loc, err=""),
            stat_loc)


def slice_max_n(size: int, n_elem: int):
    """
    Yield start and end indices to preserve max size equal to `n_elem`.

    :param size: number of element in sequence.
    :param n_elem: Number of elements to yield.
    :return: generator.
    """
    for start, end in zip(range(0, size, n_elem), range(n_elem, size + n_elem, n_elem)):
        yield start, end


def starts_with_zero_timestamp(stat_loc):
    """Check if statistics starts at 1970-01-01"""
    reader = labours.ProtobufReader()
    reader.read(stat_loc)
    start, _ = reader.get_header()
    return datetime.utcfromtimestamp(start).strftime("%Y-%m-%d") == "1970-01-01"


def merge_statistics(filenames: Sequence[Tuple[ReportStat, str]], output_filepath: str,
                     hercules_exec: str = HERCULES_EXEC, n_samples: int = 0) -> str:
    """
    Merge statistics for multiple repositories together.

    :param filenames: list of results from repository_statistics for each repository.
    :param output_filepath: path to store results.
    :param hercules_exec: location of hercules executable.
    :param n_samples: number of samples in one.
    :return: location aggregated statistics or None in case of error.
    """
    # filter out failed repositories
    locations = [loc for _, loc in filenames if loc]

    filtered_locations = []
    for loc in locations:
        if not starts_with_zero_timestamp(loc):
            filtered_locations.append(loc)
        else:
            log.warning(f"Bad date at repository {loc}.")
    # merge statistics
    file_stack = filtered_locations
    if n_samples > 0:
        # hierarchical processing
        file_cnter = 0  # used to write temporal files
        new_stack = []
        with tempfile.TemporaryDirectory(prefix="hercules_merge_") as tmp_dir:
            while len(file_stack) > n_samples:
                for start, end in slice_max_n(len(file_stack), n_samples):
                    new_tmp_loc = os.path.join(tmp_dir, f"{file_cnter}.pb")
                    new_stack.append(merge_statistics_(filenames=file_stack[start:end],
                                                       output_filename=new_tmp_loc,
                                                       hercules_exec=hercules_exec,
                                                       output_dir=tmp_dir))
                    file_cnter += 1
                    log.info(f"Number of merges: {file_cnter}")
                file_stack = [loc for loc in new_stack if loc and os.path.getsize(loc) > 0]
                new_stack = []

            return merge_statistics_(filenames=file_stack,
                                     output_filename=output_filepath,
                                     hercules_exec=hercules_exec)
    return merge_statistics_(filenames=file_stack,
                             output_filename=output_filepath,
                             hercules_exec=hercules_exec)


def merge_statistics_(filenames: Sequence[Tuple[ReportStat, str]],
                      output_filename: str = AGGREGATED_STATISTICS_NAME,
                      hercules_exec: str = HERCULES_EXEC, output_dir: str = None) -> str:
    """
    Merge statistics for multiple repositories together.

    :param filenames: list of results from repository_statistics for each repository.
    :param output_dir: directory to store results. File will be `output_dir/filename`.
    :param output_filename: name (not path) of the file with extension to store results.
    :param hercules_exec: location of hercules executable.
    :return: location aggregated statistics or None in case of error.
    """
    if output_dir:
        stat_loc = os.path.join(output_dir, output_filename)
    else:
        stat_loc = output_filename
    locations = filenames

    cmd = [hercules_exec, "combine"]
    cmd.extend(locations)
    try:
        with open(stat_loc, "wb") as f:
            subprocess.run(cmd, check=True, stdout=f)
    except subprocess.CalledProcessError as e:
        err = f"Aggregating of statistics failed with exception {e}"
        log.error(err)
        return None
    return stat_loc


def repository_statistics_multiprocessing(kwargs) -> (ReportStat, str):
    """
    Wrapper to call `repository_statistics` from `multiprocessing.Pool`.

    :param kwargs: dictionary of arguments for `repository_statistics`.
    :return: result from `repository_statistics`.
    """
    return repository_statistics(**kwargs)


def hercules_handler(input_csv: str, output: str, size_limit: int, force: bool, n_cores: int,
                     hercules_exec: str, directory_field_name: str, url_field_name: str,
                     aggregated_statistics_name: str, n_samples: int) -> None:
    """
    Pipeline to calculate statistics for multiple repositories & merge statistics together.

    :param input_csv: path to CSV with information about repositories location.
    :param output: output directory to store statistics.
    :param size_limit: max size of repo to process in bytes. If <= 0 no filtering will be applied.
    :param force: force overwriting of existing statistics (aggregated statistics will always be
                  overwritten).
    :param n_cores: how many cores to use.
    :param hercules_exec: hercules executable location.
    :param directory_field_name: name of directory field in CSV (it contains path to repository).
    :param directory_field_name: name of URL field in CSV (it contains repository's URL).
    :param aggregated_statistics_name: name of file to store aggregated statistics.
    :param n_samples: Max number of repos to combine together - it will be done in a hierarchical
                      manner. If <= 0 - no hierarchical processing will be used.
    """
    # load list of repositories to process
    repos = pd.read_csv(input_csv)
    if directory_field_name not in repos.columns:
        raise ValueError(f"Input CSV should have column \"{directory_field_name}\" but got "
                         f"{repos.columns}")
    if url_field_name not in repos.columns:
        raise ValueError(f"Input CSV should have column \"{url_field_name}\" but got "
                         f"{repos.columns}")
    # prepare arguments for parallel processing
    arguments = []
    repositories = repos[directory_field_name].tolist()
    urls = repos[url_field_name].tolist()
    for repo, url in zip(repositories, urls):
        arguments.append({"repo_loc": repo, "repo_url": url, "output_dir": output, "force": force,
                          "hercules_exec": hercules_exec, "size_limit": size_limit})
    # calculate statistics
    n_cores = n_cores if n_cores > 0 else multiprocessing.cpu_count()
    with Pool(n_cores) as p:
        results = []
        for res in tqdm.tqdm(p.imap_unordered(repository_statistics_multiprocessing,
                                              arguments),
                             total=len(repositories)):
            results.append(res)
    # merge statistics
    log.info("Start merging of statistics...")
    result_filepath = os.path.join(output, aggregated_statistics_name)
    final_stat = merge_statistics(filenames=results, output_filepath=result_filepath,
                                  hercules_exec=hercules_exec, n_samples=n_samples)
    if final_stat:
        log.info("Success!")
        log.info(f"Aggregated statistics is stored at {final_stat}")


def add_hercules_args(parser: argparse.ArgumentParser):
    parser.add_argument("-i", "--input-csv", help="Path to csv with repositories.", required=True)
    parser.add_argument("-o", "--output", help="Path to the directory where to store reports.",
                        required=True)
    parser.add_argument("--hercules-exec", default=HERCULES_EXEC,
                        help="Hercules executable.")
    parser.add_argument("-n", "--n-cores", type=int, default=N_CORES,
                        help="How many cores to use.")
    parser.add_argument("-s", "--size-limit", default=SIZE_LIMIT, type=int,
                        help="Max size of repo to process in bytes. If <= 0 no filtering will be "
                             "applied.")
    parser.add_argument("-f", "--force", action="store_true",
                        help="Force overwriting of existing statistics (summary will always be "
                             "overwritten).")
    parser.add_argument("--n-samples", default=-1, type=int,
                        help="Max number of repos to combine together - it will be done in a "
                             "hierarchical manner. If <= 0 - no hierarchical processing will be "
                             "used.")
    parser.add_argument("--directory-field-name", default=DIRECTORY_FIELD_NAME,
                        help="Name of directory field in CSV (it contains path to repository).")
    parser.add_argument("--url-field-name", default=URL_FIELD_NAME,
                        help="Name of URL field in CSV (GitHub URL).")
    parser.add_argument("--aggregated-statistics-name", default=AGGREGATED_STATISTICS_NAME,
                        help="Name of file to store aggregated statistics.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", default="INFO", choices=log._nameToLevel,
                        help="Logging verbosity.")
    add_hercules_args(parser)
    args = parser.parse_args()
    log.getLogger().setLevel(args.log_level)
    hercules_kwargs = filter_kwargs(vars(args), hercules_handler)
    hercules_handler(**hercules_kwargs)
