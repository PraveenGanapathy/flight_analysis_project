# check_kaggle_config.py
import os
import json

def check_kaggle_setup():
    # Check if kaggle.json exists
    kaggle_path = os.path.expanduser('~/.kaggle/kaggle.json')
    if not os.path.exists(kaggle_path):
        print("Error: kaggle.json not found in ~/.kaggle/")
        return False
    
    # Check file permissions
    permissions = oct(os.stat(kaggle_path).st_mode)[-3:]
    if permissions != '600':
        print(f"Warning: kaggle.json permissions are {permissions}, should be 600")
    
    # Check content format
    try:
        with open(kaggle_path) as f:
            config = json.load(f)
            if 'username' not in config or 'key' not in config:
                print("Error: kaggle.json missing username or key")
                return False
            print("Kaggle config format is correct")
            print(f"Username: {config['username']}")
            return True
    except Exception as e:
        print(f"Error reading kaggle.json: {str(e)}")
        return False

if __name__ == "__main__":
    check_kaggle_setup()