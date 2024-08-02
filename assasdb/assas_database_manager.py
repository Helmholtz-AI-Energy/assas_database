import pandas
import logging
import numpy
import os
import shutil
import uuid
import pathlib

from datetime import datetime
from typing import List, Tuple, Union

from .assas_database_handler import AssasDatabaseHandler
from .assas_database_storage import AssasStorageHandler
from .assas_astec_handler import AssasAstecHandler
from .assas_database_hdf5 import AssasDatasetHandler
from .assas_database_dataset import AssasDataset
from .assas_database_handler import AssasDocumentFile, AssasDocumentFileStatus

logger = logging.getLogger('assas_app')

class AssasDatabaseManager:

    def __init__(
        self,
        config: dict
    ) -> None:
        
        self.database_handler = AssasDatabaseHandler(config)
        self.storage_handler = AssasStorageHandler(config)
        self.astec_handler = AssasAstecHandler(config)
       
    def add_archives_to_database(
        self,
        archive_path_list: List[str]
    ) -> None:        
        
        lists_of_saving_time = self.astec_handler.get_lists_of_saving_times(archive_path_list)
        
        for idx, archive_path in enumerate(archive_path_list):
        
            system_uuid = uuid.uuid4()
            system_date = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
            system_path = str(archive_path)
            system_result = ''
            system_size = '{:.2f}'.format(AssasAstecHandler.get_size_of_archive_in_giga_bytes(len(lists_of_saving_time[idx])))
            system_user = 'User'
            system_download = 'Download'
            
            if len(lists_of_saving_time[idx]) == 1:
                system_status=AssasDocumentFileStatus.FAILED
            else:
                system_status=AssasDocumentFileStatus.UPLOADED
                
            document = AssasDocumentFile()
            document.set_system_values(
                system_uuid=str(system_uuid),
                system_date=system_date,
                system_path=system_path,
                system_result=system_result,
                system_size=system_size,
                system_user=system_user,
                system_download=system_download,
                system_status=system_status
            )       
                
            self.add_database_entry(document.get_document())
            
    def convert_archive(
        self,
        archive_path: str,
        result_path: str
    )-> bool:
        
        if self.astec_handler.convert_to_hdf5(archive_path, result_path):
            
            self.database_handler.update_file_document_path(archive_path, {'system_result':result_path})
            self.database_handler.update_file_document_path(archive_path, {'system_status':AssasDocumentFileStatus.CONVERTED})
            
            document = self.database_handler.get_file_document_path(archive_path)
            
            document = AssasDatasetHandler.update_meta_data(document) 
            self.database_handler.update_file_document_path(archive_path, document)
            
            return True
        else:
            return False
    
    def update_meta_data(
        self,
        archive_path: str,
        result_path: str = None
    )-> bool:

        if result_path is not None:
            self.database_handler.update_file_document_path(archive_path, {'system_result':result_path})
        
        document = self.database_handler.get_file_document_path(archive_path)
        
        document_file = AssasDocumentFile(document)
        document_file = AssasDatasetHandler.update_meta_data(document_file)
        
        self.database_handler.update_file_document_path(archive_path, document_file.get_document())
    
    def process_archive(
        self,
        zipped_archive_path: str
    ) -> bool:
        
        success = False
        archive_dir = os.path.dirname(zipped_archive_path)
        logger.info(f'start processing archive {archive_dir}')
        
        if self.astec_handler.unzip_archive(zipped_archive_path):
            if self.astec_handler.convert_archive(archive_dir):
                success = True       
            
        return success
    
    def process_unzipped_archive(
        self, 
        archive_path: str
    ) -> bool:
        
        success = False
        logger.info(f'start processing archive {archive_path}')
        
        success = self.astec_handler.convert_archive(archive_path)
        
        if success:
            
            system_uuid = uuid.uuid4()
            system_date = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
            system_path = archive_path
            system_size = self.storage_handler.get_size_of_archive_in_bytes(archive_path)
            system_user = 'User'
            system_download = 'Download'
            
            document = AssasDocumentFile()
            document.set_system_values(
                system_uuid=str(system_uuid),
                system_date=system_date,
                system_path=system_path,
                system_size=system_size,
                system_user=system_user,
                system_download=system_download,
                system_status=AssasDocumentFileStatus.CONVERTED
            )       
            
        return success
    
    def store_local_archive(
        self, 
        uuid: str
    ) -> None:
        
        logger.info("store dataset for uuid %s", uuid)
        
        archive_dir = self.storage_handler.local_archive + uuid + '/result/'
        self.storage_handler.create_dataset_archive(archive_dir)
        
        dataset_file_document = AssasDocumentFile.get_test_document_file(uuid, archive_dir)
        
        self.database_handler.insert_file_document(dataset_file_document)
        
        dataset = AssasDataset('test', 1000)
        
        dataset_handler = AssasDatasetHandler(dataset_file_document, dataset)
        dataset_handler.create_hdf5() 
    
    def synchronize_archive(
        self, 
        system_uuid: str
    ) -> bool:
        
        success = False
        
        if self.storage_handler.store_archive_on_share(system_uuid):
            if self.storage_handler.delete_local_archive(system_uuid):
                success = True

        return success
    
    def clear_archive(
        self, 
        system_uuid: str
    ) -> bool:
        
        success = False
        
        if self.storage_handler.delete_local_archive(system_uuid):
            success = True
            
        return success
    
    def add_test_database_entry(
        self, 
        system_uuid: str, 
        system_path: str
    ) -> None:
        
        dataset_file_document = AssasDocumentFile.get_test_document_file(system_uuid, system_path)
        
        logger.info(f'insert test document {dataset_file_document}')
                                                    
        self.database_handler.insert_file_document(dataset_file_document)
        
        logger.info(f'inserted test document {dataset_file_document}')
        
    def add_database_entry(
        self, 
        document: str
    ) -> None:
        
        print(f'insert document {document}')
        
        self.database_handler.insert_file_document(document) 
        
    def get_database_entries(
        self
    ) -> pandas.DataFrame:
        
        file_collection = self.database_handler.get_file_collection()
        
        data_frame = pandas.DataFrame(list(file_collection.find()))
        
        logger.info(f'load data frame with size {str(data_frame.size), str(data_frame.shape)}')
        
        if data_frame.size == 0:
            return data_frame
        
        data_frame['system_index'] = range(1, len(data_frame) + 1)    
        data_frame['_id'] = data_frame['_id'].astype(str)

        return data_frame
    
    def drop(
        self
    )-> None:
        
        self.database_handler.drop_file_collection()
        
    def get_database_entry(
        self, 
        id: str
    ):
        
        return self.database_handler.get_file_document(id)
    
    def get_database_entry_uuid(
        self, 
        uuid: str
    ):
        
        return self.database_handler.get_file_document_uuid(uuid)
    
