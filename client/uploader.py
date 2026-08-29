from client.manifest import ManifestDB
from client.scanner import Scanner
import requests

scanner = Scanner()
manifest = ManifestDB()
def synch_files():
    files = scanner.get_file_status()
    changed_files = [file for file in files if file["changed"] is True]
    unchanged_files = [file for file in files if file["changed"] is False]
    #print(changed_files)
    for file in changed_files:
        with open(file["full_path"], "rb") as f:
            response = requests.post(
                "http://127.0.0.1:8000/upload",
                files={"file": f},
                data={"file_hash": file["current_hash"]},
            )
        print(response.json())
        if response.json["status"] == "success":
            manifest.mark_as_synched(file["full_path"],file["current_hash"])
    

synch_files()