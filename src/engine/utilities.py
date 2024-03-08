import os
import sqlite3


def create_schema(path: str, name: str) -> int:
    """Checks if an sqlite3 file exists at the path, creates one if not.

    Args:
        path (str): file path. E.g., C:\\Users\\me\\Documents
        name (str): file name. E.g., mydata`

    Returns:
        int: returns an exit code.
    """
    if os.path.exists(path+name):
        return 1
    else:
        try:
            con = sqlite3.connect(name)
            con.close()
            return 0
        except:
            return -1
        
        
def drop_schema(path: str, name: str) -> int:
    """Checks if an sqlite3 file exists at the path, deletes it.

    Args:
        path (str): file path. E.g., C:\\Users\\me\\Documents
        name (str): file name. E.g., mydata`

    Returns:
        int: 0: file deleted successfully; 1: file not found; -1: error deleting file
    """
    if os.path.exists(path+name):
        try:
            os.remove(path+name)
            return 0
        except:
            return -1
    else:
        return 1