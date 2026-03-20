import os
from huggingface_hub import HfApi
from dotenv import load_dotenv

load_dotenv()
api = HfApi(token=os.getenv('HF_TOKEN'))

readme_content = """---
configs:
- config_name: default
  data_files:
  - split: train
    path: "**/*.parquet"
---
# Dhravani Dataset

Automatically synced audio dataset.
"""

with open('README.md', 'w') as f:
    f.write(readme_content)

print("Uploading README.md configuration...")
api.upload_file(
    path_or_fileobj='README.md',
    path_in_repo='README.md',
    repo_id='shreeshacharya/Dhravani',
    repo_type='dataset'
)
print("README uploaded successfully!")
