
import os 
import pandas
import logging
import uuid
import pathlib
import pickle
import subprocess

from uuid import uuid4
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Union


from assasdb.assas_database_handler import AssasDatabaseHandler
from assasdb.assas_database_handler import AssasDocumentFile, AssasDocumentFileStatus
from assasdb.assas_odessa_netcdf4_converter import AssasOdessaNetCDF4Converter

logger = logging.getLogger('assas_app')

class AssasAstecArchive:
    
    def __init__(
        self,
        upload_uuid: uuid4,
        name: str,
        date: str,
        user: str,
        description: str,
        archive_path: str,
        result_path: str,
    ) -> None:
        
        self._upload_uuid = upload_uuid
        self._name = name
        self._date = date
        self._user = user
        self._description = description
        self._archive_path = archive_path
        self._result_path = result_path

    @property
    def upload_uuid(self):
        return self._upload_uuid
 
    @property
    def name(self):
        return self._name
 
    @property
    def date(self):
        return self._date
    
    @property
    def user(self):
        return self._user
    
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
        lsdf_archive: str = '/mnt/ASSAS/upload_test',
        upload_directory: str = '/mnt/ASSAS/upload_test',
        connection_string: str = 'mongodb://localhost:27017/',
    ) -> None:

        self.lsdf_archive = Path(lsdf_archive)
        self.upload_directory = Path(upload_directory)

        self.database_handler = AssasDatabaseHandler(connection_string)


    def get_database_entry_by_upload_uuid(
        self,
        upload_uuid: uuid4
    ):
        
        return self.database_handler.get_file_document_by_upload_uuid(upload_uuid)
    
    def get_database_entry_by_id(
        self, 
        id: str
    ):
        
        return self.database_handler.get_file_document(id)
    
    def get_database_entry_by_uuid(
        self, 
        uuid: uuid4
    ):
        
        return self.database_handler.get_file_document_by_uuid(uuid)
    
    def get_database_entry_by_path(
        self, 
        path: str
    ):
        
        return self.database_handler.get_file_document_by_path(path)
    
    def get_all_database_entries(
        self
    ) -> pandas.DataFrame:
        
        file_collection = self.database_handler.get_file_collection()
        
        data_frame = pandas.DataFrame(list(file_collection.find()))
        
        logger.info(f'Load data frame with size {str(data_frame.size), str(data_frame.shape)}')
        
        if data_frame.size == 0:
            return data_frame
        
        data_frame['system_index'] = range(1, len(data_frame) + 1)
        data_frame['_id'] = data_frame['_id'].astype(str)

        return data_frame
    
    def set_document_status_by_uuid(
        self,
        uuid: uuid4,
        status: AssasDocumentFileStatus
    )-> None:
        
        update = {f'system_status': f'{str(status)}'}
        logger.info(f'Update file document with uuid {uuid} with update string {update}')
        
        document = self.database_handler.get_file_document_by_uuid(uuid)
        system_uuid = document['system_uuid']
        logger.info(f'Found document with uuid {system_uuid}')
                
        self.database_handler.update_file_document_by_uuid(uuid, update)
        logger.info(f'Update file document with uuid {uuid} and set status to {status}')
    
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
    
    @staticmethod
    def convert_from_bytes(
        num: float,
        blocksize: float = 1024.0
    )-> str:
        '''
        This function will convert kilobytes to MB, GB, and TB.
        '''
        
        logger.info(f'Num {num}')
        
        for x in ['B', 'KB', 'MB', 'GB', 'TB']:
            
            if num < blocksize:
                
                converted_string = f'{round(num, 2)} {x}'
                logger.info(f'Converted into {converted_string}')
                
                return converted_string
            
            num /= blocksize
    
    @staticmethod
    def get_size_of_directory_in_bytes(
        directory: str
    )-> float:
        logger.info(f'Get size of {directory}')
        return float(subprocess.check_output(['du', '-sb', directory]).split()[0])
    
    def update_archive_sizes(
        self
    )-> bool:
        
        success = False
        
        documents = self.database_handler.get_file_documents_by_status(AssasDocumentFileStatus.UPLOADED)
        document_file_list = [AssasDocumentFile(document) for document in documents]
        
        if len(document_file_list) == 0:
            
            logger.info(f'No archives in state UPLOADED present')
        
        try:
            
            for document_file in document_file_list:
            
                archive_size = AssasDatabaseManager.get_size_of_directory_in_bytes(document_file.get_value('system_path'))
                converted_size = AssasDatabaseManager.convert_from_bytes(archive_size)
            
                document_file.set_value('system_size', converted_size)
                document_file.set_value('system_status', AssasDocumentFileStatus.VALIDATED)
                self.database_handler.update_file_document_by_path(document_file.get_value('system_path'), document_file.get_document())
                
            success = True
        
        except Exception as exception:
            
            logger.error(f'Error during update of archive sizes occured: {exception}')
            
        return success

    def get_upload_uuids(
        self,
    )-> List[uuid4]:
        
        upload_uuid_list = []
        
        for directory in os.listdir(self.upload_directory):
            if os.path.isdir(Path.joinpath(self.upload_directory, directory)):
                if os.path.isfile(Path.joinpath(self.upload_directory, directory, directory)):
                    logger.debug(f'Detected complete uploaded archive {Path.joinpath(self.upload_directory, directory)}')
                    try:
                        upload_uuid_list.append(uuid.UUID(directory))
                    except ValueError:
                        logger.error('Received univalid uuid')
        
        logger.debug(f'Read {len(upload_uuid_list)} upload uuids {upload_uuid_list} in {self.upload_directory}')
      
        return upload_uuid_list
    
    @staticmethod
    def file_size(
        file_path: str
    )-> str:
        """
        this function will return the file size
        """
        if os.path.isfile(file_path):
            
            file_info = os.stat(file_path)
            logger.info(f'File size: {file_info.st_size} Os.path.getsize: {os.path.getsize(file_path)}')
            
            converted_in_bytes = AssasDatabaseManager.convert_from_bytes(file_info.st_size)
            logger.info(f'Converted in bytes {converted_in_bytes}')
            
            return converted_in_bytes
        
        if os.path.isdir(file_path):
            
            raise NotImplementedError(f'Path {file_path} points to a directory')
    
    def get_uploaded_archives_to_process(
        self
    )-> List[AssasAstecArchive]:

        registered_archive_list = []
        upload_uuid_list = self.get_upload_uuids()

        for upload_uuid in upload_uuid_list:
        
                documents = self.database_handler.get_file_document_by_upload_uuid(upload_uuid)

                if documents is None:
                
                    logger.info(f'Detect new upload with upload_uuid {str(upload_uuid)}')
                    
                    archive_list = self.read_upload_info(upload_uuid)
                    registered_archive_list.extend(archive_list)
            
                else:
                
                    logger.info(f'Upload_uuid is already processed {str(upload_uuid)}')
                    
        return registered_archive_list
    
    def process_uploads(
        self,
    )-> bool:
        
        success = False
        
        try:
            
            archive_list = self.get_uploaded_archives_to_process()
            
            if len(archive_list) == 0:
                logger.info('No new archives present')
            else:
                self.register_archives(archive_list)
            
            success = True
                   
        except Exception as exception:
            
            logger.error(f'Error when processing uploads occured: {exception}')
                
        return success
    
    def update_upload_info(
        self,
        upload_uuid: uuid4,
        key: str,
        value_list: List[str],
        upload_info_file_name: str = 'upload_info.pickle'
    )-> bool:
    
        success = False
        
        try:
            
            upload_info = {}
            upload_info_file = Path.joinpath(self.lsdf_archive, str(upload_uuid), upload_info_file_name)
            logger.info(f'Update upload info from file {str(upload_info_file)}')
        
            with open(upload_info_file, 'rb') as file:
                upload_info = pickle.load(file)
            
            logger.info(f'Upload information:')
            for key, value in upload_info.items():
                logger.info(f'{key}: {value}')
            
            logger.info(f'Update key {key} with value {value}')
            upload_info[key] = value_list
        
            with open(upload_info_file, 'wb+') as file:
                pickle.dump(upload_info, file)
            
            logger.info(f'Updated upload information:')
            for key, value in upload_info.items():
                logger.info(f'{key}: {value}')
                
            success = True
            
        except Exception as exception:
            
            logger.error(f'Error when updating upload information in file {str(upload_info_file)} occured: {exception}')
                
        return success
    
    def read_upload_info(
        self,
        upload_uuid: uuid4,
        upload_info_file_name: str = 'upload_info.pickle'
    )-> List[AssasAstecArchive]:
        
        archive_list = []
        
        upload_info = {}
        upload_info_file = Path.joinpath(self.lsdf_archive, str(upload_uuid), upload_info_file_name)
        logger.info(f'Update upload info from file {str(upload_info_file)}')
        
        with open(upload_info_file, 'rb') as file:
            upload_info = pickle.load(file)

        date = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        name = upload_info['name']
        
        archive_path = Path.joinpath(self.lsdf_archive, str(upload_uuid))
        logger.info(f'Upload location for this archive: {str(archive_path)}')
        
        for idx, archive_sub_path in enumerate(upload_info['archive_paths']):
            
            archive_list.append(AssasAstecArchive(
                upload_uuid = str(upload_uuid),
                name = f'{name}_{idx}',
                date = date,
                user = upload_info['user'],
                description = upload_info['description'],
                archive_path = str(archive_path) + archive_sub_path,
                result_path = str(archive_path) + archive_sub_path + '/../result/dataset.h5', # Put result next to binary
            ))
        
        return archive_list
    
    def register_archives(
        self,
        archive_list: List[AssasAstecArchive]
    ) -> None:

        logger.info('Start registering archives')

        archive_path_list = [archive.archive_path for archive in archive_list]
        lists_of_saving_time = AssasOdessaNetCDF4Converter.get_lists_of_saving_times(archive_path_list)

        for idx, archive in enumerate(archive_list):

            if len(lists_of_saving_time[idx]) == 1:

                system_status=AssasDocumentFileStatus.CORRUPTED
                logger.error(f'Archive is corrupted, set status to CORRUPTED {archive.archive_path}')

            else:

                logger.info(f'Archive is consistent, set status to UPLOADED {archive.archive_path}')
                system_status=AssasDocumentFileStatus.UPLOADED

            document_file = AssasDocumentFile()
            number_of_samples = len(lists_of_saving_time[idx])

            document_file.set_system_values(
                system_uuid = str(uuid.uuid4()),
                system_upload_uuid = archive.upload_uuid,
                system_date = datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
                system_path = archive.archive_path,
                system_result = archive.result_path,
                system_size = f'{str(round(AssasOdessaNetCDF4Converter.get_size_of_archive_in_giga_bytes(number_of_samples), 2))} GB',
                system_user = archive.user,
                system_download = 'hdf5 file',
                system_status = system_status
            )

            document_file.set_general_meta_values(
                meta_name = archive.name,
                meta_description = archive.description
            )

            AssasOdessaNetCDF4Converter.set_general_meta_data(
                output_path = archive.result_path,
                archive_name = archive.name,
                archive_description = archive.description
            )

            document_file.set_value('system_size_hdf5', AssasDatabaseManager.file_size(archive.result_path))
            self.add_internal_database_entry(document_file.get_document())

    def conversion_in_progress(
        self
    )-> bool:
        
        documents = self.database_handler.get_file_documents_by_status(AssasDocumentFileStatus.CONVERTING)
        document_files = [AssasDocumentFile(document) for document in documents]
        
        return len(document_files) > 0

    def convert_next_validated_archive(
        self,
    )-> None:
        
        documents = self.database_handler.get_file_documents_by_status(AssasDocumentFileStatus.VALIDATED)
        document_files = [AssasDocumentFile(document) for document in documents]
        
        if len(document_files) == 0:
            logger.info(f'Found no new archive to convert')
            return
        
        document_file = document_files[0] # take first in list
        
        document_file.set_value('system_status', AssasDocumentFileStatus.CONVERTING)
        self.database_handler.update_file_document_by_path(document_file.get_value('system_path'), document_file.get_document())
        
        try:
        
            AssasOdessaNetCDF4Converter(
                input_path = document_file.get_value('system_path'),
                output_path = document_file.get_value('system_result'),
            ).convert_astec_variables_to_netcdf4()
            
            document_file.set_value('system_status', AssasDocumentFileStatus.CONVERTED)
            document_file.set_value('system_size_hdf5', AssasDatabaseManager.file_size(document_file.get_value('system_result')))
            
            self.database_handler.update_file_document_by_path(document_file.get_value('system_path'), document_file.get_document())
            
        except Exception as exception:
            
            logger.error(f'Update status to FAILED due to exception: {exception}')
            
            document_file.set_value('system_status', AssasDocumentFileStatus.FAILED)
            self.database_handler.update_file_document_by_path(document_file.get_value('system_path'), document_file.get_document())
