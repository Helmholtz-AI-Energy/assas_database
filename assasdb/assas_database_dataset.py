import numpy as np

from typing import List, Tuple, Union, Any

class AssasDataset:
    
    def __init__(
        self,
        name: str,
        samples: int
    )-> None:
        '''
        TODO: @JD Find generic implementation for variables and dimension
        '''
        
        self.name = name
        self.variables = ['pressure', 'voidf', 'temp', 'sat_temp']
        self.channels = 4
        self.meshes = 16
        self.samples = samples
        self.data = {}        
        for variable in self.variables:
            self.data[variable] = np.zeros((self.channels, self.meshes, samples))
    
    def get_data(
        self
    )-> Any:
        
        return self.data
    
    def get_data_for_variable(
        self,
        variable: str
    )-> Any:
        
        return self.data[variable]
    
    def get_variables(
        self
    )-> List[str]:
        
        return self.variables
    
    def get_no_channels(
        self
    )-> int:
        
        return self.channels
    
    def get_no_meshes(
        self
    )-> int:
        
        return self.meshes
    
    def get_no_samples(
        self
    )-> int:
        
        return self.samples
    
    def get_name(
        self
    )-> str:
        
        return self.name
    
    def insert_data_point(
        self,
        variable: str,
        channel: int,
        mesh: int,
        sample: int,
        value: float
    )-> None:
        
        self.data[variable][channel][mesh][sample] = value
        