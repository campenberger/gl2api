import logging


def loggingFactory(module=None):
    """
    Factory method that returns a logger generation function for the given module.

    The module name is appended to the project name

        _getLogger=loggingFactory('util.alstore')
        logger=_getLogger('method')
        logger.debug('Bla')
        >>> adr.util.alstore.method DEBUG Bla
    """
    prefix="graylog_config" if module is None else "graylog_config."+module
    
    def getLogger(method=None):
        name=prefix+("."+method if method else "")
        return logging.getLogger(name)

    return getLogger


class _GenericObject(object):

    def get(self, attr, *args):
        return getattr(self, attr, *args)


def dict_to_obj(d, name='new'):
    """Convert. a dictionary into a data object."""
    top_class = type(name, (_GenericObject,), d)
    top=top_class()
    seqs = tuple, list, set, frozenset
    for i, j in d.items():
        if isinstance(j, dict):
            setattr(top, i, dict_to_obj(j))
        elif isinstance(j, seqs):
            setattr(
                top, 
                i, 
                type(j)(dict_to_obj(sj) if isinstance(sj, dict) else sj for sj in j)
            )
        else:
            setattr(top, i, j)

    return top
