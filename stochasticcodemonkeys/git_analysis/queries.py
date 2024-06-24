import sqlite3
import math
import glob
import os
import re
from collections import namedtuple
from datetime import datetime, timedelta


FileInfo = namedtuple('FileInfo', ['file', 'commits', 'complexity', 'score', 'age'])
DB_NAME = 'git_log.db'

def set_db_name(db_name):
    global DB_NAME
    DB_NAME = db_name

def _get_git_db_connection():
    global DB_NAME
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()    
    return conn, c

# Query 1: Changes by Author
def get_contributions_by_author(since=None):
    conn, cursor = _get_git_db_connection()
    
    query = """
    SELECT author, COUNT(DISTINCT commit_hash) as total_commits
    FROM commits
    """
    
    # Adding date filter if 'since' is provided
    if since:
        query += f" WHERE date >= '{since}'"
    
    query += " GROUP BY author"
    query += " ORDER BY total_commits DESC"  # Add sort by total_commits in descending order
    
    # Execute the query
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    return result
    


# Query 2: Changes by File
def get_changes_by_file(since=None):
    conn, cursor = _get_git_db_connection()
    
    # Base query to get unique commits count per file
    query = """
    SELECT file, COUNT(DISTINCT commit_hash) as total_unique_commits
    FROM commits
    """
    
    # Adding date filter if 'since' is provided
    if since:
        query += f" WHERE date >= '{since}'"
    
    query += " GROUP BY file"
    query += " ORDER BY total_unique_commits DESC"  
    
    # Execute the query
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    return result

# Query 3: Contributions Over Time
def get_contributions_over_time(startAt=None):
    conn, cursor = _get_git_db_connection()
    
    # Base query to get unique commits count per day
    query = """
    SELECT DATE(timestamp) as date, COUNT(DISTINCT commit_hash) as total_unique_commits
    FROM commits
    """

    # Adding date filter if 'startAt' is provided
    if startAt:
        query += f" WHERE date >= '{startAt}'"
    
    query += " GROUP BY date"
    
    # Execute the query
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()

# Query 4: Aggregate Commits by Directory
# maybe wanna do this by time, too
def aggregate_commits_by_directory(since=None, num_levels=None):
    file_list = get_changes_by_file(since)
    dir_commits = {}
    dir_count = 0
    
    # Regular expression to match the pattern {old => new}
    pattern = r'\{([^}]+)\s=>\s([^}]*)\}'

    for file, commit_count in file_list:
        # Check if the file name contains the pattern and extract the new path
        match = re.search(pattern, file)
        if match:
            file = re.sub(pattern, match.group(1), file)

        full_path = os.path.dirname(file)
        if num_levels is not None:
            path_parts = full_path.split(os.sep)
            directory = os.sep.join(path_parts[:num_levels])
        else:
            directory = full_path
        
        if directory not in dir_commits:
            dir_commits[directory] = 0
            dir_count += 1
        dir_commits[directory] += commit_count
    
    for directory, commit_count in dir_commits.items():
        print(f"{directory} - {commit_count} commits")
    
    return dir_commits, dir_count

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
            print("\tSkipping non-code file:", file)
            non_code_file_count += 1
    print(f"\tSkipped non-code files: {non_code_file_count}")
    return results

def is_code_file(file):
    # Determine if a file is a code file based on its extension
    code_extensions = {'.py', '.js', '.java', '.cpp', '.c', '.h', '.cs', '.rb', '.php', '.html', '.css', '.jsx', '.ts', '.tsx'}
    _, ext = os.path.splitext(file)
    return ext in code_extensions

def get_commit_ages():
    conn, cursor = _get_git_db_connection()
    
    query = '''
    SELECT f.file, MIN(julianday('now') - julianday(c.timestamp)) as commit_age
    FROM files f
    JOIN commits c ON f.file = c.file
    GROUP BY f.file
    '''

    results = cursor.execute(query).fetchall()
    conn.close()
    
    return {row[0]: int(row[1]) for row in results}



def determine_hotspot_data(file_commits, file_complexities, commit_ages, score_func=None):
    SIX_MONTHS = 180
    if score_func is None:
        score_func = lambda cm, cp, a: cm * cp if a < SIX_MONTHS else math.log(cm * cp, a)

    results = []
    max_score = 0
    for file, commits in file_commits:
        commit_age = commit_ages.get(file, 1) + 1
        file_complexity = file_complexities.get(file, 0)
        if file_complexity == 0:
            pass
        else:
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

def run_hotspot_analysis(source_path):
    file_commits = get_changes_by_file()
    file_complexities = calculate_file_complexity(source_path)
    commit_ages = get_commit_ages()
    
    return determine_hotspot_data(file_commits, file_complexities, commit_ages)
    
def create_bus_factor_chart(db_path, hotspots):
    conn, cursor = _get_git_db_connection(db_path)
    
    top_hotspots = get_top_hotspots(hotspots)
    hotspot_files = [f['file'] for f in top_hotspots]

    query = '''
    SELECT file, author, datetime(timestamp) as datetime
    FROM commits
    WHERE file IN ({})
    '''.format(','.join('?' for _ in hotspot_files))
    
    cursor.execute(query, hotspot_files)
    rows = cursor.fetchall()
    
    recent_date = datetime.now() - timedelta(days=365)
    recent_data = [row for row in rows if row[1] != 'GitHub' and datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S') >= recent_date]
    
    file_author_map = {}
    for row in recent_data:
        file, author, _ = row
        if file not in file_author_map:
            file_author_map[file] = set()
        file_author_map[file].add(author)
    
    bus_factor_text = ""
    for file, authors in file_author_map.items():
        if len(authors) == 1:
            bus_factor_text += f"{file:<50} - {list(authors)[0]}\n"
    
    if bus_factor_text != "":
        print("\n🚌 Hotspots with a high bus factor:")
        print(bus_factor_text)
    
    conn.close()

def get_top_hotspots(hotspots):
    # Assuming hotspots is a list of FileInfo named tuples
    # This function should sort and return the top hotspots based on the score
    sorted_hotspots = sorted(hotspots, key=lambda x: x.score, reverse=True)
    top_hotspots = []
    for hotspot in sorted_hotspots:
        if hotspot.score > 0:
            top_hotspots.append(hotspot)
    return top_hotspots


def create_bus_factor(source_path):
    # Get hotspot data by running the hotspot analysis
    hotspots = run_hotspot_analysis(source_path)
    top_hotspots = get_top_hotspots(hotspots)
    hotspot_files = [f.file for f in top_hotspots]
    hotspot_lookup = {f.file: f for f in top_hotspots}  

    conn, cursor = _get_git_db_connection()

    query = '''
    SELECT file, author, datetime(timestamp) as datetime
    FROM commits
    WHERE file IN ({})
    '''.format(','.join('?' for _ in hotspot_files))
    
    cursor.execute(query, hotspot_files)
    rows = cursor.fetchall()
    
    recent_date = datetime.now() - timedelta(days=365)
    recent_data = [row for row in rows if row[1] != 'GitHub' and datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S') >= recent_date]
    
    file_author_map = {}
    for row in recent_data:
        file, author, _ = row
        if file not in file_author_map:
            file_author_map[file] = set()
        file_author_map[file].add(author)
    
    bus_factor_text = ""
    for file, authors in file_author_map.items():
        data = hotspot_lookup[file]
        if len(authors) > 0:
            bus_factor_text += f"{file:<50}\tcommits:{data.commits}\tcomplexity:{data.complexity}\tscore:{data.score}\tage:{data.age}\t{list(authors)}\n"
    
    if bus_factor_text != "":
        print("\n🚌 Hotspots with a high bus factor:")
        print(bus_factor_text)
    
    conn.close()



def run_queries(full_path, repo_name):
    # Run the queries
    print("Contributions by Author:")
    get_contributions_by_author()

    print("\nChanges by File:")
    get_changes_by_file()

    print("\nCommits By Day:")
    get_contributions_over_time()

    print("\nAggregated Commits by Directory:")
    aggregate_commits_by_directory(num_levels=2)

    hotspot_data = run_hotspot_analysis(full_path)
    print("Hotspot Data:")
    for file_info in hotspot_data:
        print(f"{file_info.file} - Commits: {file_info.commits}, Complexity: {file_info.complexity}, "
              f"Score: {file_info.score}, Age: {file_info.age}")

    create_bus_factor(full_path)