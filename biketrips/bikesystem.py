"""
bike system tags and selector
"""
from biketrips.bixi import Bixi


def selector(args):
    """
    select relevant class given bike system
    """
    if args['bike_sys'] == 'bixi':
        trip = Bixi(args)
    return trip
