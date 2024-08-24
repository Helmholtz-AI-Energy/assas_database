import os
import sys
import zipfile
import glob
import logging
import subprocess
import numpy
import h5py

from .assas_database_dataset import AssasDataset

from typing import List, Tuple, Union

logger = logging.getLogger('assas_app')

class AssasAstecHandler:
    
    def __init__(
        self,
        config: dict
    ) -> None:
        
        self.command = [f'{config.PYTHON_VERSION}', f'{config.ASTEC_ROOT}', f'{config.ASTEC_PARSER}']
        self.config = config
        
        astec_python_interface_location = os.path.join(config.ASTEC_ROOT, 'code', 'proc')
        if astec_python_interface_location not in sys.path:
            sys.path.append(astec_python_interface_location)
        
        astec_python_location = os.path.join(config.ASTEC_ROOT, 'code', 'bin', config.ASTEC_COMPUTER + '-' + config.ASTEC_COMPILER, 'wrap_python')
        if astec_python_location not in sys.path:
            sys.path.append(astec_python_location)
            
        import AstecParser # needed by astec (class from astec.py)
        import astec # contains definition of class Astec which will set all environment variables
        
        AP = AstecParser.AstecParser() # initialize a parser of a fictive command line
        AP.parsed_arguments.compiler=config.ASTEC_COMPILER # replace the default compiler by the one you want
        A = astec.Astec(AP) # make an instance of Astec
        A.set_environment() # initialize all environment variables
    
    def get_lists_of_saving_times(
        self,
        archive_dir_list: List[str],
    ) -> List[List[str]]:
        
        result_list = []

        import pyastec as pa # pyastec can now be loadded
        pa.astec_init()
        
        for archive_dir in archive_dir_list:
            try:
                saved_instants = pa.tools.get_list_of_saving_time_from_path(archive_dir)
                result_list.append(saved_instants)
            except:
                result_list.append([-1])
       
        pa.end()       
        
        return result_list
    
    @staticmethod
    def get_size_of_archive_in_giga_bytes(        
        number_of_timesteps: int,
        size_of_saving: float = 1.68
    ) -> float:
        
        size_in_giga_bytes = (number_of_timesteps * size_of_saving) / 1000.0
        
        return size_in_giga_bytes
    
    def read_astec_archives(
        self,
        archive_path_list: List[str]      
    )-> List[AssasDataset]:
        
        import pyastec as pa # pyastec can now be loadded
        pa.astec_init()
        
        result_list = []
        
        for idx, archive_path in enumerate(archive_path_list):
            
            saved_instants = pa.tools.get_list_of_saving_time_from_path(archive_path)
            logger.info(f'Time list {saved_instants}')
                    
            dataset = AssasDataset(archive_path, len(saved_instants))
            
            logger.info(f'Start data collection for {archive_path}')
            
            index = 0
            for t, base in pa.tools.save_iterator(archive_path):
                    
                logger.info(f'Process index {str(index)}, time {t}')
                
                #DATA1  
                for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):            
                    if row == 0:
                        value = base.get('LOADTIME')
                    else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core1:MESH '+str(row))-1)+':THER :P')[1]
                    
                    dataset.insert_data_point('pressure', 1, row, index, value)
            
                #DATA2 
                for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                    if row == 0:
                        value = base.get('LOADTIME')
                    else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core2:MESH '+str(row))-1)+':THER :P')[1]
                    
                    dataset.insert_data_point('voidf', 2, row, index, value)            
                
                #DATA3  
                for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                    if row == 0:
                        value = base.get('LOADTIME')
                    else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core3:MESH '+str(row))-1)+':THER :P')[1]
                    
                    dataset.insert_data_point('temp', 3, row, index, value)      
                
                #DATA4  
                for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                    if row == 0:
                        value = base.get('LOADTIME')
                    else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core0:MESH '+str(row))-1)+':THER :P')[1]
                    
                    dataset.insert_data_point('sat_temp', 0, row, index, value)      
                
                #DATA5   
                for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                    if row == 0:
                        value = base.get('LOADTIME')
                    else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core1:MESH '+str(row))-1)+':THER :P')[1]
                    
                    dataset.insert_data_point('pressure', 1, row, index, value)      
                    
                #DATA6   
                for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                    if row == 0:
                        value = base.get('LOADTIME')
                    else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core2:MESH '+str(row))-1)+':THER :P')[1]
                    
                    dataset.insert_data_point('voidf', 2, row, index, value)      
                    
                #DATA7   
                for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                    if row == 0:
                        value = base.get('LOADTIME')
                    else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core3:MESH '+str(row))-1)+':THER :P')[1]
                    
                    dataset.insert_data_point('temp', 3, row, index, value)      
                    
                #DATA8   
                for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                    if row == 0:
                        value = base.get('LOADTIME')
                    else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core0:MESH '+str(row))-1)+':THER :P')[1]
                    
                    dataset.insert_data_point('sat_temp', 0, row, index, value)      
                
                #DATA9   
                for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                    if row == 0:
                        value = base.get('LOADTIME')
                    else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core1:MESH '+str(row))-1)+':THER :P')[1]
                    
                    dataset.insert_data_point('pressure', 1, row, index, value)      
                    
                #DATA10   
                for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                    if row == 0:
                        value = base.get('LOADTIME')
                    else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core2:MESH '+str(row))-1)+':THER :P')[1]
                    
                    dataset.insert_data_point('voidf', 2, row, index, value)      
                    
                #DATA11   
                for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                    if row == 0:
                        value = base.get('LOADTIME')
                    else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core3:MESH '+str(row))-1)+':THER :P')[1]
                    
                    dataset.insert_data_point('temp', 3, row, index, value)      
                    
                #DATA12   
                for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                    if row == 0:
                        value = base.get('LOADTIME')
                    else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core0:MESH '+str(row))-1)+':THER :P')[1]
                    
                    dataset.insert_data_point('sat_temp', 0, row, index, value)      
                
                #DATA13   
                for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                    if row == 0:
                        value = base.get('LOADTIME')
                    else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core1:MESH '+str(row))-1)+':THER :P')[1]
                    
                    dataset.insert_data_point('pressure', 1, row, index, value)      
                    
                #DATA14   
                for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                    if row == 0:
                        value = base.get('LOADTIME')
                    else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core2:MESH '+str(row))-1)+':THER :P')[1]
                    
                    dataset.insert_data_point('voidf', 2, row, index, value)      
                    
                #DATA15   
                for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                    if row == 0:
                        value = base.get('LOADTIME')
                    else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core3:MESH '+str(row))-1)+':THER :P')[1]
                    
                    dataset.insert_data_point('temp', 3, row, index, value)      
                    
                #DATA16   
                for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                    if row == 0:
                        value = base.get('LOADTIME')
                    else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core0:MESH '+str(row))-1)+':THER :P')[1]
                    
                    dataset.insert_data_point('sat_temp', 0, row, index, value)         
                    
                logger.info(f'Index number {str(index)} out of {str(len(saved_instants)-1)}')
                index += 1
            
            result_list.append(dataset)
        
        pa.end()
        
        return result_list

    def read_astec_archive(
        self,
        astec_archive_path: str
    )-> AssasDataset:
        
        import pyastec as pa # pyastec can now be loadded
        pa.astec_init()
        
        logger.info(f'Start reading binary {astec_archive_path}')
        
        saved_instants = pa.tools.get_list_of_saving_time_from_path(astec_archive_path)
        logger.info(f'Time list {saved_instants}')
                
        dataset = AssasDataset(astec_archive_path, len(saved_instants))
        
        logger.info(f'Start data collection for {astec_archive_path}')
        
        index = 0
        for t, base in pa.tools.save_iterator(astec_archive_path):
                
            logger.info(f'Process index {str(index)}, time {t}')
            
            #DATA1  
            for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):            
                if row == 0:
                    value = base.get('LOADTIME')
                else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core1:MESH '+str(row))-1)+':THER :P')[1]
                
                dataset.insert_data_point('pressure', 1, row, index, value)
        
            #DATA2 
            for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                if row == 0:
                    value = base.get('LOADTIME')
                else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core2:MESH '+str(row))-1)+':THER :P')[1]
                
                dataset.insert_data_point('voidf', 2, row, index, value)            
            
            #DATA3  
            for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                if row == 0:
                    value = base.get('LOADTIME')
                else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core3:MESH '+str(row))-1)+':THER :P')[1]
                
                dataset.insert_data_point('temp', 3, row, index, value)      
            
            #DATA4  
            for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                if row == 0:
                    value = base.get('LOADTIME')
                else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core0:MESH '+str(row))-1)+':THER :P')[1]
                
                dataset.insert_data_point('sat_temp', 0, row, index, value)      
            
            #DATA5   
            for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                if row == 0:
                    value = base.get('LOADTIME')
                else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core1:MESH '+str(row))-1)+':THER :P')[1]
                
                dataset.insert_data_point('pressure', 1, row, index, value)      
                
            #DATA6   
            for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                if row == 0:
                    value = base.get('LOADTIME')
                else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core2:MESH '+str(row))-1)+':THER :P')[1]
                
                dataset.insert_data_point('voidf', 2, row, index, value)      
                
            #DATA7   
            for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                if row == 0:
                    value = base.get('LOADTIME')
                else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core3:MESH '+str(row))-1)+':THER :P')[1]
                
                dataset.insert_data_point('temp', 3, row, index, value)      
                
            #DATA8   
            for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                if row == 0:
                    value = base.get('LOADTIME')
                else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core0:MESH '+str(row))-1)+':THER :P')[1]
                
                dataset.insert_data_point('sat_temp', 0, row, index, value)      
            
            #DATA9   
            for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                if row == 0:
                    value = base.get('LOADTIME')
                else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core1:MESH '+str(row))-1)+':THER :P')[1]
                
                dataset.insert_data_point('pressure', 1, row, index, value)      
                
            #DATA10   
            for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                if row == 0:
                    value = base.get('LOADTIME')
                else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core2:MESH '+str(row))-1)+':THER :P')[1]
                
                dataset.insert_data_point('voidf', 2, row, index, value)      
                
            #DATA11   
            for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                if row == 0:
                    value = base.get('LOADTIME')
                else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core3:MESH '+str(row))-1)+':THER :P')[1]
                
                dataset.insert_data_point('temp', 3, row, index, value)      
                
            #DATA12   
            for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                if row == 0:
                    value = base.get('LOADTIME')
                else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core0:MESH '+str(row))-1)+':THER :P')[1]
                
                dataset.insert_data_point('sat_temp', 0, row, index, value)      
            
            #DATA13   
            for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                if row == 0:
                    value = base.get('LOADTIME')
                else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core1:MESH '+str(row))-1)+':THER :P')[1]
                
                dataset.insert_data_point('pressure', 1, row, index, value)      
                
            #DATA14   
            for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                if row == 0:
                    value = base.get('LOADTIME')
                else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core2:MESH '+str(row))-1)+':THER :P')[1]
                
                dataset.insert_data_point('voidf', 2, row, index, value)      
                
            #DATA15   
            for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                if row == 0:
                    value = base.get('LOADTIME')
                else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core3:MESH '+str(row))-1)+':THER :P')[1]
                
                dataset.insert_data_point('temp', 3, row, index, value)      
                
            #DATA16   
            for row in range(len(base.get('VESSEL:DISC:AXIA')) - 1):
                if row == 0:
                    value = base.get('LOADTIME')
                else: value = base.get('VESSEL:MESH '+str(base.get('VESSEL:CHANNEL core0:MESH '+str(row))-1)+':THER :P')[1]
                
                dataset.insert_data_point('sat_temp', 0, row, index, value)         
                
            logger.info(f'Index number {str(index)} out of {str(len(saved_instants)-1)}')
            index += 1
        
        pa.end()
           
        return dataset
        
    def execute_parser_script(
        self,
        archive_dir: str
    ) -> bool:
        
        success = False
        
        try:
            current_dir = os.getcwd()
            logger.info(f'Current working directory is {current_dir}')

            logger.info(f'Change to archive directory {archive_dir}')
            os.chdir(archive_dir + '/archive')
            
            logger.info(f'Run assas astec parser with command {self.command}')
            with subprocess.Popen(self.command) as process: process.wait()
                            
            logger.info(f'Changed back to current_dir: {current_dir}')
            os.chdir(current_dir)
            
            success = True
        
        except:
            logger.exception(f'Exception occurred during conversion')
        
        return success
        
    @staticmethod
    def unzip_archive(
        zipped_archive_path: str
    ) -> bool:
        
        unzipped_archive_dir = os.path.dirname(zipped_archive_path) + '/archive'
        
        try:
            with zipfile.ZipFile(zipped_archive_path, 'r') as zip:
                
                zip.extractall(unzipped_archive_dir)
        except:
            return False
        
        return True
            
    @staticmethod
    def get_astec_archive(
        archive_dir: str
    )-> str:
        
        logger.info(f'Archive directory: {archive_dir}')
        
        zipped_dir = glob.glob(archive_dir + '/*.zip')
        
        logger.info(f'Archive directory: {zipped_dir}')
        
        if len(zipped_dir) != 1:
            raise ValueError('No or more than one archive present')
            return
        
        return zipped_dir[0]