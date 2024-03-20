#!/usr/bin/env python3

import os
import h5py
import logging
import pyodessa as pyod

import assas_database_dataset as add

from pathlib import Path
from datetime import datetime

logger = logging.getLogger('assas_app')

SEP = ':'
MAX_DEPTH = 50

SC0_type = h5py.string_dtype('utf-8', 8)
T_type   = h5py.string_dtype('utf-8', 255)

class AssasAstecParser:
    
    def __init__(self, archive_name):
        
        self.astec_archive_name = archive_name
        self.astec_archive_dir = Path(os.getcwd())
        self.result_dir = f'{self.astec_archive_dir.parent.absolute()}/result'
        self.result_file = f'{self.result_dir}/dataset.h5'
                
        print(self.result_dir)
        
        if not os.path.exists(self.result_dir):
            os.mkdir(self.result_dir)
    
    @staticmethod
    def create_hdf5(file_path, dataset: add.AssasDataset):
        
        with h5py.File(file_path, 'w') as h5file:
            
            h5file.create_group('metadata')
            h5file['metadata'].attrs['name'] = dataset.get_name()
            h5file['metadata'].attrs['channels'] = dataset.get_no_channels()
            h5file['metadata'].attrs['meshes'] = dataset.get_no_meshes()
            h5file['metadata'].attrs['samples'] = dataset.get_no_samples()

            data_group = h5file.create_group('data')
                
            for variable in dataset.get_variables():
                group = data_group.create_group(variable)
                array = dataset.get_data_for_variable(variable)
                group.create_dataset(variable, data = array)

        h5file.close()       

    @staticmethod
    def read_binary(astec_archive_dir):
        
        print(f'start reading binary {astec_archive_dir}')
        
        pyod.init()
        
        binary_file = pyod.open(os.path.join(astec_archive_dir,'SavingIndex'))
        
        index = pyod.restore(binary_file, 0.)
        saved_instants = [saving.get('time') for saving in index.family('SAVING')]
        print(f'found {len(saved_instants)} saved instants')
        
        dataset = add.AssasDataset(astec_archive_dir, len(saved_instants))
        
        print(f'start data collection for {astec_archive_dir}')
        
        for index, item in enumerate(saved_instants):
                
            base = pyod.restore(astec_archive_dir, saved_instants[index])
            
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
                
            print(f'Index number {str(index)} out of {str(len(saved_instants)-1)}')
            
        pyod.close(binary_file)
        
        return dataset
        
    def convert_to_hdf5(self):
        
        print(f'convert archive in {self.astec_archive_dir} to hdf5 format')
            
        print(f'read binary archive in {self.astec_archive_dir}')
        self.dataset = self.read_binary(self.astec_archive_name)
            
        print(f'create hdf5 file {self.result_file}')
        self.create_hdf5(self.result_file, self.dataset)

if __name__ == '__main__':
    
    '''
    TODO: @JD Get rid of the workarround
    '''
    
    archive_dir = os.getcwd()
    file_list = os.listdir(archive_dir)
    file_list.remove('results')
    
    print(file_list)
    
    if len(file_list) != 1:
        raise ValueError('no or more than one archive present')
    else:
        archive_name = file_list[0]
        print(f'archive name {archive_name}')
        astec_parser = AssasAstecParser(archive_name)
        astec_parser.convert_to_hdf5()

