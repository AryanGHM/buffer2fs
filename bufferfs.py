import os, subprocess
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

    def on_any_event(self, event):
        print('triggered: ', event)

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
                self.target_size_pool[dir] -= spsize
                os.system(f'powershell -command mv \'{sp}\' \'{dir}\'')
                return
        
        # Couldn't move file to any of the directories, either file is too big
        # or all target directories are almost at the limit.
        freeavg = 0
        for dir, free in self.target_size_pool.items():
            freeavg += free
        
        freeavg /= len(self.target_size_pool.items())
        if freeavg <= 0.1:
            raise RuntimeError(f"""Couldn't write new file to any of target folders,
                    target free average is too low: {freeavg}\nsrc_path: {sp}""")
        else:
            print("Couldn't write data to any of folders, file is too big. \nsrc_path: ", sp)

    def on_created(self, event: Union[FileCreatedEvent, DirCreatedEvent]):
        self.move(event.src_path)


if __name__ == "__main__":
    obs = Observer()
    handler = bufferh(['.\\targets\\1\\', '.\\targets\\2\\'], 1)
    obs.schedule(handler, '.\\buffer', recursive=False)
    obs.start()
    try:
        while 1:
            sleep(1)
    except:
        obs.stop()
        obs.join()