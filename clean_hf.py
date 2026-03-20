import os
from huggingface_hub import HfApi
from dotenv import load_dotenv

load_dotenv()
hf_token = os.getenv('HF_TOKEN')
api = HfApi(token=hf_token)
repo_id = "shreeshacharya/Dhravani"

try:
    # List files in the repo
    print(f"Scanning {repo_id} for metadata.jsonl files...")
    files = api.list_repo_files(repo_id=repo_id, repo_type="dataset")

    # Find metadata.jsonl files
    files_to_delete = [f for f in files if f.endswith('metadata.jsonl')]

    if files_to_delete:
        print(f"Found {len(files_to_delete)} files to delete. Deleting...")
        for file in files_to_delete:
            api.delete_file(path_in_repo=file, repo_id=repo_id, repo_type="dataset")
            print(f"Deleted {file}")
    else:
        print("No metadata.jsonl files found to delete.")
except Exception as e:
    print(f"Error: {e}")
