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

# define constants for parsing
SEP = ":"
MAX_DEPTH = 50

SC0_type = h5py.string_dtype('utf-8', 8)
T_type   = h5py.string_dtype('utf-8', 255)

def convert_structure_recusively(pybase, h5base, lvl = 0):
    if lvl > MAX_DEPTH:
        raise RecursionError("Too much depth in input file")

    for i, family_name in enumerate(pybase.families()):
        stru_quantity = pyas.odcard(pybase._od_obj, family_name)
        for k in range(stru_quantity):
            # Some datatypes are not implemented yet (SR2) and are skipped
            try:
                s = pybase.get(f"{family_name} {k}")
            except KeyError:
                print(f"Skipping {family_name}")
                continue

            typ = pyas.odbase_type(pybase._od_obj, family_name)
            if typ == pyas.OD_BASE:
                grp = h5base.create_group(f"{family_name}{SEP}{k}")
                convert_structure_recusively(s, grp, lvl=lvl+1)
            elif typ == pyas.OD_R0:
                dataset = h5base.create_dataset(f"{family_name}{SEP}{k}", data=s, dtype='f')
            elif typ == pyas.OD_R1:
                array = np.array(s)
                dataset = h5base.create_dataset(f"{family_name}{SEP}{k}", data=array, dtype='f')
            elif typ == pyas.OD_I0:
                dataset = h5base.create_dataset(f"{family_name}{SEP}{k}", data=s, dtype='i4')
            elif typ == pyas.OD_I1:
                array = np.array(s)
                dataset = h5base.create_dataset(f"{family_name}{SEP}{k}", data=array, dtype='i4')
            elif typ == pyas.OD_T:
                s = bytes(s, "iso_8859_1").decode("utf-8", "ignore")
                dataset = h5base.create_dataset(f"{family_name}{SEP}{k}", data=s, dtype=T_type)
            elif typ == pyas.OD_C0:
                dataset = h5base.create_dataset(f"{family_name}{SEP}{k}", data=s, dtype=SC0_type)
            elif typ == pyas.OD_C1:
                array = [c0 for c0 in s]
                dataset = h5base.create_dataset(f"{family_name}{SEP}{k}", data=array, dtype=SC0_type)
            elif typ == pyas.OD_RG:
                keys = [k for k in s.keys()]
                values = np.array([s[k] for k in s.keys()])
                dataset = h5base.create_dataset(f"{family_name}{SEP}RG_C{k}", data=keys, dtype=SC0_type)
                dataset = h5base.create_dataset(f"{family_name}{SEP}RG_R{k}", data=values, dtype='f')
            elif typ == pyas.OD_IG:
                keys = [k for k in s.keys()]
                values = np.array([s[k] for k in s.keys()])
                dataset = h5base.create_dataset(f"{family_name}{SEP}IG_C{k}", data=keys, dtype=SC0_type)
                dataset = h5base.create_dataset(f"{family_name}{SEP}IG_R{k}", data=values, dtype='i4')
            else:
                print(f"{type(s)} : {family_name}[{k}] = {s}")
                if isinstance(s, pyas.PyOd_rg):
                    dico = {}
                    print(f">> {family_name}[{k}] = {s}")
                    for k in s.keys():
                        dico[k] = s[k]

def convert_structure_reduced(dir):
    
    print("convert reduced")
    # Variable names
    Name_of_binary_folder = dir
    base_setup = []
    
    # Time steps in binary files folders
    pyod.init()
    
    binary_file = pyod.open(os.path.join(Name_of_binary_folder,"SavingIndex"))
    index = pyod.restore(binary_file, 0.)
    saved_instants = [saving.get('time') for saving in index.family('SAVING')]
    print(saved_instants)
    
    # New directory created to store data
    path = os.getcwd()+"/result"
    if not os.path.exists(path):
        os.mkdir(path)
        
    # init assas dataset
    name = Name_of_binary_folder
    variables = ["pressure", "voidf", "temp", "sat_temp"]
    channels = 4
    meshes = 16
    samples = len(saved_instants)
        
    dataset = add.AssasDataset(name, variables, channels, meshes, samples)
    
    print("------------------------------------------------------")
    print("------------------DATA SECTION------------------------")
    for index, item in enumerate(saved_instants):
            
        base = pyod.restore(Name_of_binary_folder, saved_instants[index])
        
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
            
    with h5py.File(path+"/dataset_reduced.h5","w") as h5file:
        
        h5file.create_group('metadata')
        h5file['metadata'].attrs['name'] = name
        h5file['metadata'].attrs['upload_time'] = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        h5file['metadata'].attrs['creation_time'] = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        h5file['metadata'].attrs['uuid'] = str(uuid.uuid4())

        h5file.create_group('input')
        h5file['input'].attrs['debris'] = 0

        data_group = h5file.create_group('data')
            
        for variable in variables:
            group = data_group.create_group(variable)
            array = dataset.get_data_for_variable(variable)
            group.create_dataset(variable, data = array)

    h5file.close()       
        
    print("------------------------------------------------------")

def convert_full(dir):
    
    print("convert full")
        
    # Variable names
    Name_of_binary_folder = dir
    base_setup = []
    
    # Time steps in binary files folders
    pyod.init()
    
    times = pyas.tools.get_list_of_saving_time_from_path(Name_of_binary_folder)
       
    binary_file = pyod.open(os.path.join(Name_of_binary_folder,"SavingIndex"))
    index = pyod.restore(binary_file, 0.)
    saved_instants = [saving.get('time') for saving in index.family('SAVING')]
    print(saved_instants)
    
    # New directory created to store data
    path = os.getcwd()+"/results"
    if not os.path.exists(path):
        os.mkdir(path)
    
    for t, base in pyas.tools.save_iterator(Name_of_binary_folder, t_start=times[-1], t_end=times[-1]):
        print(f" time = {t}s")
        with h5py.File(f"{path}/dataset_{t:.7f}_full.h5", 'w') as h5file:
            convert_structure_recusively(base, h5file)
            
    pyod.close(binary_file)

if __name__ == '__main__':
    
    cwd = os.getcwd()
    archive_dir = cwd + "/archive/"
    print(os.listdir(archive_dir))    
    dir = archive_dir + os.listdir(archive_dir)[0]
    print("convert from archive %s", dir)
   
    convert_structure_reduced(dir)

