import threading
import time
import os

from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver, PollingObserverVFS

from watchdog.events import FileSystemEventHandler, LoggingEventHandler, FileSystemEvent

class Handler(FileSystemEventHandler):
    
    def __init__(self,*args, **kwargs):
        super().__init__()

    def on_created(
        self,
        event: FileSystemEvent
    )-> None:
        
        print('Received event')
        
        if event.is_directory and event.event_type == 'created':
            print(f'Detected new folder {event.src_path}')
        else:
            print(f'Other event detected {event.src_path}')


class AssasUploadChecker:
    
    def __init__(
        self,
        interval: int = 3,
        directory: str = '/mnt/ASSAS/upload_test'
    ) -> None:
        
        self.interval = interval
        
        self.observer = PollingObserver()
        self.directory = directory
        
        #self.timer_runs = threading.Event()
        #self.timer_runs.set()
        
        #check_thread = threading.Thread(target=self.check_for_new_archives, args=(self.timer_runs,))
        #check_thread.start()
        
    def run(self):
        
        event_handler = Handler()
        self.observer.schedule(event_handler, self.directory, recursive=True)
        self.observer.start()
        
        print(f'start observer at {self.directory}')
        
        try:
            while True:
                print('Run')
                time.sleep(3)
        except KeyboardInterrupt:
            self.observer.stop()
            print("Error, stop observer")

        self.observer.join()
    
    def check_for_new_archives(
        self,
        timer_runs: threading.Event
    ):
        while timer_runs.is_set():
            
            print('Check for new archives')         
            
            time.sleep(self.interval)
            
            
    def stop_checking(
        self
    ):
        self.timer_runs.clear()


if __name__ == "__main__":
    
    upload_checker = AssasUploadChecker()
    #upload_checker.run()
    #time.sleep(10)
    
    #upload_checker.stop_checking()
