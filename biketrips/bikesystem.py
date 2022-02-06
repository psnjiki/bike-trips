"""
bike system tags and selector
"""
from biketrips import operators as ops


SYS_LIST = ['bixi', 'bsto', 'cabi', 'citi']


def selector(args):
    """
    select relevant class given bike system
    """
    if args['bike_sys'] == 'bixi':
        trip = ops.bixi.Bixi(args)
    elif args['bike_sys'] == 'bsto':
        trip = ops.bsto.Bsto(args)
    elif args['bike_sys'] == 'cabi':
        trip = ops.cabi.Cabi(args)
    elif args['bike_sys'] == 'citi':
        trip = ops.citi.Citi(args)
    else:
        msg = 'Wrong value for --bike-sys. {} not found in [{}]'
        msg = msg.format(args['bike_sys'], ', '.join(SYS_LIST))
        raise Exception(msg)
    return trip
