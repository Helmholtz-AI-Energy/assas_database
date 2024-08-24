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
from .assas_database_hdf5 import AssasHdf5DatasetHandler
from .assas_database_dataset import AssasDataset
from .assas_database_handler import AssasDocumentFile, AssasDocumentFileStatus

logger = logging.getLogger('assas_app')

class AssasAstecArchive:
    
    def __init__(
        self,
        name: str,
        group: str,
        date: str,
        creator: str,
        description: str,
        archive_path: str,
        result_path: str
    ) -> None:
        
        self._name = name
        self._group = group
        self._date = date
        self._creator = creator
        self._description = description
        self._archive_path = archive_path
        self._result_path = result_path
        
    @property
    def name(self):
        return self._name
    
    @property
    def group(self):
        return self._group
    
    @property
    def date(self):
        return self._date
    
    @property
    def creator(self):
        return self._creator
    
    @property
    def description(self):
        return self._description
    
    @property
    def archive_path(self):
        return self._archive_path
    
    @property
    def result_path(self):
        return self._result_path

class AssasDatabaseManager:

    def __init__(
        self,
        config: dict
    ) -> None:
        
        self.database_handler = AssasDatabaseHandler(config)
        self.astec_handler = AssasAstecHandler(config)
    
    def get_database_entry_by_id(
        self, 
        id: str
    ):
        
        return self.database_handler.get_file_document(id)
    
    def get_database_entry_by_uuid(
        self, 
        uuid: str
    ):
        
        return self.database_handler.get_file_document_uuid(uuid)
    
    def get_database_entry_by_path(
        self, 
        path: str
    ):
        
        return self.database_handler.get_file_document_path(path)
    
    def get_all_database_entries(
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
    
    def add_internal_database_entry(
        self, 
        document: dict
    ) -> None:
        
        logger.info(f'Insert document {document}')        
        self.database_handler.insert_file_document(document)
        
    def empty_internal_database(
        self
    )-> None:
        
        self.database_handler.drop_file_collection()
    
    def register_archives(
        self,
        archive_list: List[AssasAstecArchive]
    ) -> None:        
        
        archive_path_list = [archive.archive_path for archive in archive_list]        
        lists_of_saving_time = self.astec_handler.get_lists_of_saving_times(archive_path_list)
        
        for idx, archive in enumerate(archive_list):
     
            if len(lists_of_saving_time[idx]) == 1:
                system_status=AssasDocumentFileStatus.CORRUPTED
            else:
                system_status=AssasDocumentFileStatus.UPLOADED
                
            document_file = AssasDocumentFile()
            
            document_file.set_system_values(
                system_uuid=str(uuid.uuid4()),
                system_date=datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
                system_path=archive.archive_path,
                system_result=archive.result_path,
                system_size='{:.2f}'.format(AssasAstecHandler.get_size_of_archive_in_giga_bytes(len(lists_of_saving_time[idx]))),
                system_user='User',
                system_download='Download',
                system_status=system_status
            )
            
            document_file.set_general_meta_values(
                meta_name=archive.name,
                meta_group=archive.group,
                meta_date=archive.date,
                meta_creator=archive.creator,
                meta_description=archive.description                                
            )
            
            dataset = AssasDataset(archive.name, len(lists_of_saving_time[idx]))
            
            document_file.set_meta_data_values(
                meta_data_variables='[' + ' '.join(f'{variable}' for variable in dataset.get_variables()) + ']',
                meta_data_channels=dataset.get_no_channels(),
                meta_data_meshes=dataset.get_no_meshes(),
                meta_data_samples=dataset.get_no_samples()
            )
            
            #AssasHdf5DatasetHandler.write_meta_data_to_hdf5(document_file)
                
            self.add_internal_database_entry(document_file.get_document())

    def convert_archives_to_hdf(
        self,
        archive_list: List[AssasAstecArchive]
    )-> None:
        
        success = False
        
        try:
            
            archive_path_list = [archive.archive_path for archive in archive_list]
            datasets = self.astec_handler.read_astec_archives(archive_path_list)
            
            for idx, archive in enumerate(archive_list):
                
                document = self.database_handler.get_file_document_path(archive.archive_path)
                document_file = AssasDocumentFile(document)
                
                if document_file.get_value('system_status') == AssasDocumentFileStatus.UPLOADED:
                
                    logger.info(f'Create hdf5 file {archive.result_path}')                
                    #AssasHdf5DatasetHandler.write_data_into_hdf5(archive.result_path, datasets[idx])
                                
                    document_file.set_value('system_status', AssasDocumentFileStatus.CONVERTED)            
                    self.database_handler.update_file_document_path(archive.archive_path, document_file.get_document())
                
                else:
                    
                    system_path = document_file.get_value('system_path')
                    logger.warning(f'Astec archive under {system_path} is corrupt')
            
            success = True
            
        except:
            
            logger.exception(f'Exception occurred during conversion')
        
        return success
  
    def convert_archive_to_hdf5(
        self,
        archive: AssasAstecArchive
    )-> None:
        
        success = False
        
        try:
            
            document = self.database_handler.get_file_document_path(archive.archive_path)
            document_file = AssasDocumentFile(document)
            
            if document_file.get_value('system_status') == AssasDocumentFileStatus.UPLOADED:
                
                dataset = self.astec_handler.read_astec_archive(archive.archive_path)
                
                logger.info(f'Create hdf5 file {archive.result_path}')            
                #AssasHdf5DatasetHandler.write_data_into_hdf5(archive.result_path, dataset)
            
                document_file.set_value('system_status', AssasDocumentFileStatus.CONVERTED)            
                self.database_handler.update_file_document_path(archive.archive_path, document_file.get_document())
                
                success = True
                
            else:
                
                system_path = document_file.get_value('system_path')
                logger.warning(f'Astec archive under {system_path} is corrupt')
            
        except:

            logger.exception(f'Exception occurred during conversion')
        
        return success
            
        

