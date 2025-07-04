import threading
import tempfile
import os
import json

write_lock = threading.Lock()

def append_json_safely(data, filepath):
    with write_lock:
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as tmp:
            json.dump(data, tmp)
            tmp.write("\n")
            tmp_path = tmp.name

        with open(filepath, "a", encoding="utf-8") as f_out, open(tmp_path, "r", encoding="utf-8") as f_in:
            f_out.write(f_in.read())
        
        os.remove(tmp_path)
