import numpy

class AssasDatasetInputDeck:
    
    def __init__(self):
        self.initial_values = []

class AssasDataset:
    
    def __init__(self, name, variables, channels, meshes, samples, data):
        
        self.name = name
        self.variables = variables
        self.channels = channels
        self.meshes = meshes
        self.samples = samples        
        self.data = data
    
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
