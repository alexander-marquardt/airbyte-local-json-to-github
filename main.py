
import os, json
import git
from datetime import datetime


WEBFLOW_SOURCE_JSON_DIR = "/tmp/airbyte_local/webflow-collections"
WEBFLOW_DEST_JSON_DIR = "/Users/arm/Documents/test-webflow-backup-1"

""" 
WARNING: Before executing this code, ensure that git has been initialized in the destination directory 
as instructed below. 

Create a new repository on Github, and manually create a new git repository
inside the directory: <WEBFLOW_DEST_JSON_DIR>. Just follow instructions from github such as the following:

echo "# <your github repo name>" >> README.md
git init
git add README.md
git commit -m "first commit"
git branch -M main
git remote add origin https://github.com/alexander-marquardt/<your github repo name>
git push -u origin main

"""


def loop_over_jsonl_and_write_to_output(source_filename_with_path, destination_folder_name_with_path):

    with open(source_filename_with_path, "r") as r_file:
        for json_line in r_file:
            json_obj = json.loads(json_line)
            _airbyte_ab_id = json_obj["_airbyte_ab_id"]
            destination_file = f"{destination_folder_name_with_path}/{_airbyte_ab_id}"
            with open(destination_file, "w") as w_file:
                print(json_obj, file=w_file)


def walk_json_files():

    for root, dirs, files in os.walk(WEBFLOW_SOURCE_JSON_DIR):
        for name in files:
            source_filename_with_path = os.path.join(root, name)
            destination_folder_name= os.path.splitext(name)[0]
            destination_folder_name_with_path = f"{WEBFLOW_DEST_JSON_DIR}/{destination_folder_name}"
            if not os.path.exists(destination_folder_name_with_path):
                os.makedirs(destination_folder_name_with_path)

            loop_over_jsonl_and_write_to_output(source_filename_with_path, destination_folder_name_with_path)


def push_to_github():

    try:
        # open the git repo if it exists
        repo = git.Repo(WEBFLOW_DEST_JSON_DIR)
    except git.InvalidGitRepositoryError:
        # create the repo if doesn't exist
        print("Please create the git repository on github and then locally")

    repo.git.add(f"{WEBFLOW_DEST_JSON_DIR}/.")
    try:
        repo.git.commit(m=f"Commit by script: {__file__} on {datetime.now().isoformat()}")
    except Exception as e:
        print(f"Ignoring exception: {e}")

    print("Pushing to origin")
    repo.remotes.origin.push()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    walk_json_files()
    push_to_github()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
