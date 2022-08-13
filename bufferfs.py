import ctypes
from getopt import getopt
import os, subprocess
import sys
from time import sleep
from typing import Union
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, DirCreatedEvent

class bufferh(FileSystemEventHandler):
    def __init__(self, _targets: list, _limit: int) -> None:
        """
        @param _targets: list of target directories.
        @param _limit: limit size for each directory in GB.
        """
        self.targets = _targets
        self.limit = _limit

        # Create a size pool for target dirs, os.stat is not reliable 
        # because there could be parallel buffering underway.
        self.target_size_pool = {}
        for dir in self.targets:
            self.target_size_pool[dir] = self.limit
        super().__init__()

    def getsize(self, sp) -> float:
        """ Get size of the path. """
        if os.path.isfile(sp):
            return os.path.getsize(sp) / 1073741824 # Gigabytes
        elif os.path.isdir(sp):
            total_size = 0
            for dirpath, dirnames, filenames in os.walk(sp):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    # skip if it is symbolic link
                    if not os.path.islink(fp):
                        total_size += os.path.getsize(fp)
            return total_size / 1073741824 # Gigabytes
        
        return 0

    def move(self, sp):
        """ Selects a directory from target dirs. 
        Makes sure directory has enough space for the given file. 
        returns target dir. And moves sp to it"""
        for dir in self.targets:
            spsize = self.getsize(sp)
            if self.target_size_pool[dir] >= spsize:
                # update pool size pre-move so other processes don't violate the limit.
                print(f"Moving {sp} to {dir}, size (GB) {spsize}, target free {float(self.target_size_pool[dir])}")
                self.target_size_pool[dir] -= spsize
                os.system(f'powershell -command mv \'{sp}\' \'{dir}\'')
                return
        
        # Couldn't move file to any of the directories, either file is too big
        # or all target directories are almost at the limit.
        i = 0
        j = 0
        for dir, free in self.target_size_pool.items():
            j += 1
            if free / self.limit <= 0.1:
                i += 1
        print(f"i {i} j {j}")
        if i == j:
            print('\a')
            ctypes.windll.user32.MessageBoxW(0, \
                u"All directories are full, please empty target dirs and re-run the script.", u"bufferfs error", 0x1000)
            exit(0)

    def on_created(self, event: Union[FileCreatedEvent, DirCreatedEvent]):
        self.move(event.src_path)

def printhelp():
    print("""Usage:
 bufferfs --target-dir=.\\targets\\ --limit=100 --buffer-dir=.\\buffer

Watches buffer-dir for new files and moves new files to the least numbered target directory
under target-dir, with enough space below limit.

args:
--target-dir : A parent directory with only empty, numbered directories underneath. Example tree:\n
| targets
|| 1
||| [Empty]
|| 2
||| [Empty]
|| 3
||| [Empty]

--buffer-dir : Buffer directory all files inside directory will be split moved into target dirs.

--limit : Target directories limit of size in gigabytes. If all directories are above 90 percent occupied.
And a file is pending to move, raises an error and stops buffering.

--help : Display this message and exit.""")

if __name__ == "__main__":

    longopts = ['target-dir=', 'limit=', 'buffer-dir=', 'help']
    opts, args = getopt(sys.argv[1:], '', longopts)

    targetd = None
    limit = None
    bufferd = None
    for o, a in opts:
        if o == '--help':
            printhelp()
            exit(0)
        elif o == '--target-dir':
            targetd = a
        elif o == 'limit':
            limit = int(a)
        elif o == '--buffer-dir':
            bufferd = a
        elif o == '--limit':
            limit = int(a)

    obs = Observer()
    handler = bufferh([os.path.join(targetd, a) for a in os.listdir(targetd)], limit)
    obs.schedule(handler, bufferd, recursive=False)
    obs.start()
    try:
        while 1:
            sleep(1)
    except:
        obs.stop()
        obs.join()
        raise 