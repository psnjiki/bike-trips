"""
bike system tags and selector
"""
from biketrips import operators as ops


def selector(args):
    """
    select relevant class given bike system
    """
    if args['bike_sys'] == 'bixi':
        trip = ops.bixi.Bixi(args)
    if args['bike_sys'] == 'bsto':
        trip = ops.bsto.Bsto(args)
    if args['bike_sys'] == 'cabi':
        trip = ops.cabi.Cabi(args)
    return trip
