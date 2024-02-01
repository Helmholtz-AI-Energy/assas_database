#!/usr/bin/env python3

import os
import sys
import h5py
import numpy as np
import uuid
import shutil
from datetime import datetime

import assas_database_dataset as add

# import astec python imterface
import pyastec as pyas
import pyodessa as pyod

import logging

logger = logging.getLogger(__name__)

# define constants for parsing
SEP = ":"
MAX_DEPTH = 50

SC0_type = h5py.string_dtype('utf-8', 8)
T_type   = h5py.string_dtype('utf-8', 255)

def create_hdf5(path, name, dataset):
    
    with h5py.File(path+"/dataset.h5","w") as h5file:
        
        h5file.create_group('metadata')
        h5file['metadata'].attrs['name'] = name
        h5file['metadata'].attrs['upload_time'] = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        h5file['metadata'].attrs['creation_time'] = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        h5file['metadata'].attrs['uuid'] = str(uuid.uuid4())

        h5file.create_group('input')
        h5file['input'].attrs['debris'] = 0

        data_group = h5file.create_group('data')
            
        for variable in dataset.get_variables():
            group = data_group.create_group(variable)
            array = dataset.get_data_for_variable(variable)
            group.create_dataset(variable, data = array)

    h5file.close()       

def read_binary(name):
    
    pyod.init()
    
    binary_file = pyod.open(os.path.join(name,"SavingIndex"))
    index = pyod.restore(binary_file, 0.)
    saved_instants = [saving.get('time') for saving in index.family('SAVING')]
    logger.info(saved_instants)
    
    dataset = add.AssasDataset(name, len(saved_instants))
    
    logger.info("------------------------------------------------------")
    logger.info("------------------DATA SECTION------------------------")
    for index, item in enumerate(saved_instants):
            
        base = pyod.restore(name, saved_instants[index])
        
        #DATA1  
        for row in range(len(base.get("VESSEL:DISC:AXIA")) - 1):            
            if row == 0:
                value = base.get("LOADTIME")
            else: value = base.get("VESSEL:MESH "+str(base.get("VESSEL:CHANNEL core1:MESH "+str(row))-1)+":THER :P")[1]
            
            dataset.insert_data_point("pressure", 1, row, index, value)
    
        #DATA2 
        for row in range(len(base.get("VESSEL:DISC:AXIA")) - 1):
            if row == 0:
                value = base.get("LOADTIME")
            else: value = base.get("VESSEL:MESH "+str(base.get("VESSEL:CHANNEL core2:MESH "+str(row))-1)+":THER :P")[1]
            
            dataset.insert_data_point("voidf", 2, row, index, value)            
        
        #DATA3  
        for row in range(len(base.get("VESSEL:DISC:AXIA")) - 1):
            if row == 0:
                value = base.get("LOADTIME")
            else: value = base.get("VESSEL:MESH "+str(base.get("VESSEL:CHANNEL core3:MESH "+str(row))-1)+":THER :P")[1]
            
            dataset.insert_data_point("temp", 3, row, index, value)      
        
        #DATA4  
        for row in range(len(base.get("VESSEL:DISC:AXIA")) - 1):
            if row == 0:
                value = base.get("LOADTIME")
            else: value = base.get("VESSEL:MESH "+str(base.get("VESSEL:CHANNEL core0:MESH "+str(row))-1)+":THER :P")[1]
            
            dataset.insert_data_point("sat_temp", 0, row, index, value)      
        
        #DATA5   
        for row in range(len(base.get("VESSEL:DISC:AXIA")) - 1):
            if row == 0:
                value = base.get("LOADTIME")
            else: value = base.get("VESSEL:MESH "+str(base.get("VESSEL:CHANNEL core1:MESH "+str(row))-1)+":THER :P")[1]
            
            dataset.insert_data_point("pressure", 1, row, index, value)      
            
        #DATA6   
        for row in range(len(base.get("VESSEL:DISC:AXIA")) - 1):
            if row == 0:
                value = base.get("LOADTIME")
            else: value = base.get("VESSEL:MESH "+str(base.get("VESSEL:CHANNEL core2:MESH "+str(row))-1)+":THER :P")[1]
            
            dataset.insert_data_point("voidf", 2, row, index, value)      
            
        #DATA7   
        for row in range(len(base.get("VESSEL:DISC:AXIA")) - 1):
            if row == 0:
                value = base.get("LOADTIME")
            else: value = base.get("VESSEL:MESH "+str(base.get("VESSEL:CHANNEL core3:MESH "+str(row))-1)+":THER :P")[1]
            
            dataset.insert_data_point("temp", 3, row, index, value)      
            
        #DATA8   
        for row in range(len(base.get("VESSEL:DISC:AXIA")) - 1):
            if row == 0:
                value = base.get("LOADTIME")
            else: value = base.get("VESSEL:MESH "+str(base.get("VESSEL:CHANNEL core0:MESH "+str(row))-1)+":THER :P")[1]
            
            dataset.insert_data_point("sat_temp", 0, row, index, value)      
        
        #DATA9   
        for row in range(len(base.get("VESSEL:DISC:AXIA")) - 1):
            if row == 0:
                value = base.get("LOADTIME")
            else: value = base.get("VESSEL:MESH "+str(base.get("VESSEL:CHANNEL core1:MESH "+str(row))-1)+":THER :P")[1]
            
            dataset.insert_data_point("pressure", 1, row, index, value)      
            
        #DATA10   
        for row in range(len(base.get("VESSEL:DISC:AXIA")) - 1):
            if row == 0:
                value = base.get("LOADTIME")
            else: value = base.get("VESSEL:MESH "+str(base.get("VESSEL:CHANNEL core2:MESH "+str(row))-1)+":THER :P")[1]
            
            dataset.insert_data_point("voidf", 2, row, index, value)      
            
        #DATA11   
        for row in range(len(base.get("VESSEL:DISC:AXIA")) - 1):
            if row == 0:
                value = base.get("LOADTIME")
            else: value = base.get("VESSEL:MESH "+str(base.get("VESSEL:CHANNEL core3:MESH "+str(row))-1)+":THER :P")[1]
            
            dataset.insert_data_point("temp", 3, row, index, value)      
            
        #DATA12   
        for row in range(len(base.get("VESSEL:DISC:AXIA")) - 1):
            if row == 0:
                value = base.get("LOADTIME")
            else: value = base.get("VESSEL:MESH "+str(base.get("VESSEL:CHANNEL core0:MESH "+str(row))-1)+":THER :P")[1]
            
            dataset.insert_data_point("sat_temp", 0, row, index, value)      
        
        #DATA13   
        for row in range(len(base.get("VESSEL:DISC:AXIA")) - 1):
            if row == 0:
                value = base.get("LOADTIME")
            else: value = base.get("VESSEL:MESH "+str(base.get("VESSEL:CHANNEL core1:MESH "+str(row))-1)+":THER :P")[1]
            
            dataset.insert_data_point("pressure", 1, row, index, value)      
            
        #DATA14   
        for row in range(len(base.get("VESSEL:DISC:AXIA")) - 1):
            if row == 0:
                value = base.get("LOADTIME")
            else: value = base.get("VESSEL:MESH "+str(base.get("VESSEL:CHANNEL core2:MESH "+str(row))-1)+":THER :P")[1]
            
            dataset.insert_data_point("voidf", 2, row, index, value)      
            
        #DATA15   
        for row in range(len(base.get("VESSEL:DISC:AXIA")) - 1):
            if row == 0:
                value = base.get("LOADTIME")
            else: value = base.get("VESSEL:MESH "+str(base.get("VESSEL:CHANNEL core3:MESH "+str(row))-1)+":THER :P")[1]
            
            dataset.insert_data_point("temp", 3, row, index, value)      
            
        #DATA16   
        for row in range(len(base.get("VESSEL:DISC:AXIA")) - 1):
            if row == 0:
                value = base.get("LOADTIME")
            else: value = base.get("VESSEL:MESH "+str(base.get("VESSEL:CHANNEL core0:MESH "+str(row))-1)+":THER :P")[1]
            
            dataset.insert_data_point("sat_temp", 0, row, index, value)         
            
        print("Index number "+str(index)+" out of "+str(len(saved_instants)-1))
        
    pyod.close(binary_file)
    
    return dataset
    
def convert_to_hdf5(dir):
    
    logger.info("------------------------------------------------------")
    logger.info("convert to hdf format")
        
    path = os.getcwd()+"/result"
    if not os.path.exists(path):
        os.mkdir(path)
    
    logger.info("------------------------------------------------------")
    logger.info("read binary")
    dataset = read_binary(dir)    
        
    logger.info("------------------------------------------------------")
    logger.info("create hdf5")
    
    create_hdf5(path, dir, dataset)
        
    logger.info("------------------------------------------------------")

if __name__ == '__main__':
    
    cwd = os.getcwd()
    archive_dir = cwd + "/archive/"
    print(os.listdir(archive_dir))    
    dir = archive_dir + os.listdir(archive_dir)[0]
    print("convert from archive %s", dir)
   
    convert_to_hdf5(dir)

