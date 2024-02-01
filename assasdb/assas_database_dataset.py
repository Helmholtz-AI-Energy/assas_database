import numpy as np

class AssasDatasetInputDeck:
    
    def __init__(self):
        self.initial_values = []

class AssasDataset:
    
    def __init__(self, name, samples):
        '''
        TODO: @JD Find generic implementation for variables and dimension
        '''
        
        self.name = name
        self.variables = ["pressure", "voidf", "temp", "sat_temp"]
        self.channels = 4
        self.meshes = 16
        self.samples = samples
        self.data = {}        
        for variable in self.variables:
            self.data[variable] = np.random.rand(self.channels, self.meshes, samples, 1).reshape(self.channels, self.meshes, samples)
    
    def get_data(self):
        
        return self.data
    
    def get_data_for_variable(self, variable):
        
        return self.data[variable]
    
    def get_variables(self):
        
        return self.variables
    
    def get_no_channels(self):
        
        return self.channels
    
    def get_no_meshes(self):
        
        return self.meshes
    
    def get_no_samples(self):
        
        return self.samples
    
    def get_name(self):
        
        return self.name
    
    def insert_data_point(self, variable, channel, mesh, sample, value):
        
        self.data[variable][channel][mesh][sample] = value
        