
from collections import namedtuple

class Duration(namedtuple('Duration', 'weeks, days, hours, minutes, seconds')):
    
    def __str__(
        self
    )-> str:
        
        return ', '.join(self._get_formatted_units())

    def _get_formatted_units(
        self
    ): #TODO: fix type hint
        
        for unit_name, value in self._asdict().items():
            if value > 0:
                if value == 1:
                    unit_name = unit_name.rstrip('s')
                yield '{} {}'.format(value, unit_name)

def get_duration(
        seconds: int
    )-> Duration:
        '''
        Constructs a Duration object.
        Parameters
        ----------
        seconds : int
            Number of seconds of the measured timespan.
        Returns
        ----------
        Duration
            Object to visualize the time span.
        '''
        
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        days, hours = divmod(hours, 24)
        weeks, days = divmod(days, 7)
        
        return Duration(weeks, days, hours, minutes, seconds)