import subprocess
import os

def save_git_log(repo_name, repo_directory_path):
    # Define the command to run
    command = 'git log --reverse --all -M -C --numstat --format="^^%h--%ct--%cI--%an%n"'
    
    # Get the directory of the current script
    script_directory = os.path.dirname(os.path.abspath(__file__))
    parent_directory = os.path.dirname(script_directory)    
    
    # Define the output folder relative to the execution location
    output_folder = os.path.join(parent_directory, 'output')
    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    # Define the output file path
    output_file_path = os.path.join(output_folder, f"{repo_name}_git_log.txt")
    
    # Run the command and capture the output
    try:
        result = subprocess.run(command, shell=True, check=True, text=True, capture_output=True, cwd=repo_directory_path)
        output = result.stdout
        # Write the output to the file
        with open(output_file_path, "w", encoding="utf-8") as output_file:
            output_file.write(output)
    
        return output_file_path
    except subprocess.CalledProcessError as e:
        output = e.output
    

