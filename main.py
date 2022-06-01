
import os, json
import git
from datetime import datetime

""" 
This code is a proof-of concept that shows an example of how to drive local data that has been
downloaded with Airbyte's "Local JSON" destination connector into Github. 
See: https://docs.airbyte.com/integrations/destinations/local-json/

Background:
- Airbyte stores downloaded data as one file per stream. 
- Each file is formatted as JSONL (JSON Lines), and may contain many lines. 
- Each line inside the JSONL file is a new json record. 

To make it easier to track changes in Git, this script converts each JSONL file into a folder, 
and each line (which is also a record) becomes its own file inside the corresponding folder. After 
converting the downloaded data into the new format, the script pushes the changes to github. 

I anticipate that this script can be executed as a cron job or (eventually) via a webhook to reformat
the data downloaded by Airbyte and push it into github. 

Initially this was written to push data that is downloade from a CMS (Webflow) into Github. 

"""

AIRBYTE_LOCAL_SOURCE_JSON = "/tmp/airbyte_local/webflow-collections"
DEST_DIR_FOR_GITHUB = "/Users/arm/Documents/test-webflow-backup-1"

""" 
WARNING: Before executing this code, ensure that git has been initialized in the destination directory 
as instructed below. 

Create a new repository on Github, and manually create a new git repository
inside the directory: <DEST_DIR_FOR_GITHUB>, by following instructions given on github (such as the following):

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

    for root, dirs, files in os.walk(AIRBYTE_LOCAL_SOURCE_JSON):
        for name in files:
            source_filename_with_path = os.path.join(root, name)
            destination_folder_name= os.path.splitext(name)[0]
            destination_folder_name_with_path = f"{DEST_DIR_FOR_GITHUB}/{destination_folder_name}"
            if not os.path.exists(destination_folder_name_with_path):
                os.makedirs(destination_folder_name_with_path)

            loop_over_jsonl_and_write_to_output(source_filename_with_path, destination_folder_name_with_path)


def push_to_github():

    try:
        # open the git repo if it exists
        repo = git.Repo(DEST_DIR_FOR_GITHUB)
    except git.InvalidGitRepositoryError:
        # create the repo if doesn't exist
        print("Please create the git repository on github and then locally")

    repo.git.add(f"{DEST_DIR_FOR_GITHUB}/.")
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
