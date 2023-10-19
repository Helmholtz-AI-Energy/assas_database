import os
import uuid
import time
from datetime import datetime
import h5py
import numpy

class AssasDataset:
    
    def __init__(self, mountpoint, archive_dir, tmp_dir):
        
        self.mount_point = mountpoint
        self.archive_dir = archive_dir
        self.tmp_dir = tmp_dir
        
        self.uuid = str(uuid.uuid4())
        self.upload_time = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        self.filename = "dataset.h5"
        self.path = self.mount_point + self.archive_dir + str(self.uuid) + "/"

    def read_binary(self):

        self.name = "scenario_x"
        self.variables = ["var1", "var2"]
        self.channels = 4
        self.meshes = 16
        self.samples = 1000

    def create_lsdf_archive(self):

        if not os.path.isdir(self.mount_point + self.archive_dir):
            os.mkdir(self.mount_point + self.archive_dir)
        
        if not os.path.isdir(self.path):
            os.mkdir(self.path)
    
    def create_hdf5(self):

        with h5py.File(self.path + self.filename, 'w') as h5f:

            h5f.create_group('metadata')
            h5f['metadata'].attrs['upload_time'] = 0

            h5f.create_group('input')
            h5f['input'].attrs['debris'] = 0

            data_group = h5f.create_group('data')

            for variable in self.variables:
                variable_group = data_group.create_group(variable)
                array = numpy.random.rand(self.channels, self.meshes, self.samples, 1).reshape(self.channels, self.meshes, self.samples)
                variable_group.create_dataset(variable, data = array)

        h5f.close()

    def get_file_document(self):

        return {"uuid": self.uuid, "name": self.name, "path": self.path, "upload_time": self.upload_time}