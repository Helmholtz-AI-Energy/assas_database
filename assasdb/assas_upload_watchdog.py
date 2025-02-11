import threading
import time
import os
import logging
import requests
import pathlib
import uuid

from uuid import uuid4
from typing import List
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver, PollingObserverVFS
from watchdog.events import FileSystemEventHandler, LoggingEventHandler, FileSystemEvent

from assasdb.assas_database_manager import AssasDatabaseManager

logger = logging.getLogger('assas_app')

class Handler(FileSystemEventHandler):
    
    def __init__(
        self,
        config: dict,
    )-> None:
        self.config = config

    def on_modified(
        self, 
        event: FileSystemEvent
    )-> None:
        
        logger.info('Received event')
        
        if event.src_path == self.config.UPLOAD_FILE:
            
            logger.info(f'Detect changes in {event.src_path}')
            
            manager = AssasDatabaseManager(self.config)
            manager.process_uploads()

class AssasUploadWatchdog:
    
    def __init__(
        self,
        config: dict,
    ) -> None:
        
        self.config = config
        self.directory = self.config.UPLOAD_DIRECTORY
        
        self.observer = PollingObserver()
    
    def stop(
        self
    )-> None:
        
        logger.info(f'Stop PollingObserver at {self.directory}')
        
        self.observer.stop()        
        self.observer.join()
    
    def start(
        self
    )-> None:
        
        event_handler = Handler(self.config)
        
        self.observer.schedule(event_handler, self.directory, recursive=True)
        self.observer.start()
        
        logger.info(f'Start PollingObserver at {self.directory}')
