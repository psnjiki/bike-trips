"""
bike system tags and selector
"""
from biketrips.bixi import Bixi
from biketrips.bsto import Bsto

def selector(args):
    """
    select relevant class given bike system
    """
    if args['bike_sys'] == 'bixi':
        trip = Bixi(args)
    if args['bike_sys'] == 'bsto':
        trip = Bsto(args)
    return trip
