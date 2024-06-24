import glob
import math
import os
import sys
from collections import namedtuple
from datetime import datetime
import pandas as pd


FileInfo = namedtuple("FileInfo", "file commits complexity age score")

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def read_git_log_csv(filename):
    df = pd.read_csv(filename)
    df["file_abbr"] = df["file"].map(abbreviate_filename)
    without_github_user = df[df["author"] != "GitHub"]
    without_github_user.fillna("", inplace=True)
    return without_github_user


def abbreviate_filename(filename):
    folders = filename.split("/")
    number_of_folders = len(folders)
    if number_of_folders >= 4:
        last_folder = number_of_folders - 1
        second_last_folder = last_folder - 1
        return f"{folders[0]}/.../{folders[second_last_folder]}/{folders[last_folder]}"
    return filename


def calculate_file_complexity(source_path):
    glob_path = source_path + "/**/*.*"
    print("\n📄 Source files:")
    print(f"\tReading: {glob_path}")
    results = {}

    non_code_file_count = 0
    for file in glob.glob(glob_path, recursive=True):
        if is_code_file(file):
            file_size = os.path.getsize(file)
            relative_file = file.replace(source_path + "/", "")
            results[relative_file] = file_size
        else:
            non_code_file_count = non_code_file_count + 1
    print(f"\tSkipped non-code files: {non_code_file_count}")
    return results


def calculate_file_commits(df):
    results = {}
    file_commits = df.groupby("file")["commit_hash"].count()
    for file in file_commits.index:
        commits = file_commits.loc[file]
        results[file] = commits
    return results


def get_commit_age(df, now):
    results = {}
    commit_ages = df.groupby("file")["timestamp"].max()
    for file in commit_ages.index:
        max_date_str = commit_ages.loc[file]
        max_date = datetime.strptime(max_date_str, "%Y-%m-%dT%H:%M:%S")
        age = (now - max_date).days
        results[file] = age
    return results


def determine_hotspot_data(
    file_commits, file_complexities, commit_ages, score_func=None
):
    SIX_MONTHS = 180
    if score_func is None:
        score_func = (
            lambda cm, cp, a: cm * cp if a < SIX_MONTHS else math.log(cm * cp, a)
        )

    results = []
    max_score = 0
    for file, commits in file_commits.items():
        commit_age = commit_ages.get(file, 1) + 1
        file_complexity = file_complexities.get(file, 0)
        if file_complexity == 0:
            # print(f"can't find '{file}'")
            pass
        else:
            # print(f"found '{file}': {file_complexity}")
            if commits > 2:
                score = score_func(commits, file_complexity, commit_age)
                if score > max_score:
                    max_score = score
                new_file = FileInfo(
                    file=file,
                    commits=commits,
                    complexity=file_complexity,
                    score=score,
                    age=commit_age,
                )
                results.append(new_file)
    normalized_results = []
    for file in results:
        new_score = round((file.score / max_score) * 100, 1)
        normalized_score = FileInfo(
            file=file.file,
            commits=file.commits,
            complexity=file.complexity,
            score=new_score,
            age=file.age,
        )
        normalized_results.append(normalized_score)
    return normalized_results



def write_csv_result(repo_name, file_info):
    with open(f"output/{repo_name}_git_analysis_result.csv", "w", encoding="utf-8") as results_file:
        results_file.write("commits,complexity,file,score\n")
        for file in file_info:
            results_file.write(
                f'{file.commits},{file.complexity},"{file.file}",{file.score}\n'
            )


def is_code_file(path):
    exclude_extensions = [
        ".md",
        ".lock",
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".yaml",
        ".yml",
        ".json",
        ".xml",
        ".scss",
    ]
    _, file_extension = os.path.splitext(path)
    if file_extension in exclude_extensions:
        return False
    return True



def do_analysis(repo_name, csv_file, source_path):
    df = read_git_log_csv(csv_file)
    file_commits = calculate_file_commits(df)
    commit_ages = get_commit_age(df, datetime.now())
    print("\n📅 Commit history")
    print(f"\tRead file commits: {len(file_commits)}")

    file_complexities = calculate_file_complexity(source_path)
    print(f"\tRead file complexities: {len(file_complexities)}")

    hotspot_data = determine_hotspot_data(file_commits, file_complexities, commit_ages)
    write_csv_result(repo_name, hotspot_data)
