import os
from huggingface_hub import HfApi
from dotenv import load_dotenv

def clean_huggingface_hub_parquets():
    load_dotenv()
    hf_token = os.getenv('HF_TOKEN')
    repo_id = os.getenv('HF_REPO_ID', "shreeshacharya/Dhravani")
    
    api = HfApi(token=hf_token)
    
    print(f"Scanning {repo_id} for parquet files...")
    
    try:
        files = api.list_repo_files(repo_id=repo_id, repo_type="dataset")
        files_to_delete = [f for f in files if f.endswith('.parquet')]
        
        if files_to_delete:
            print(f"Found {len(files_to_delete)} outdated parquet files. Deleting...")
            for f in files_to_delete:
                api.delete_file(path_in_repo=f, repo_id=repo_id, repo_type="dataset")
                print(f"Deleted {f}")
        else:
            print("No `.parquet` files found to clear.")
            
        # Optional: delete .gitattributes or README.md metadata blocks if they explicitly declare features
        # We will not do this unless really necessary as HF infers features if no metadata JSONL exists
        print("Done cleaning Hugging Face Hub.")
            
    except Exception as e:
        print(f"Error during clearing HF: {e}")

if __name__ == "__main__":
    clean_huggingface_hub_parquets()
