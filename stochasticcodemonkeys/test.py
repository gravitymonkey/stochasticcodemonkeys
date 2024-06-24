
import sys
import os
from git_analysis import save_git_log, git_log_to_csv, fill_db, set_db_name, run_queries, do_analysis

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

repo_name = 'diem-backend'
full_path = '/Users/jasonuechi/dev/diem-backend'

git_log = save_git_log(repo_name, full_path)
git_log_to_csv.create_csv(
        repo_name, git_log,"\.gem|\.lock|yarn|gemfile", "dependabot|github"
    )
do_analysis(repo_name, f"output/{repo_name}.csv", full_path)
fill_db(repo_name, True)
set_db_name(f'output/{repo_name}.db')
run_queries(full_path, repo_name)
