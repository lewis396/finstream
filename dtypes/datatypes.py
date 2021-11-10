from collections import MutableMapping


class StaticDict(MutableMapping):
    """ A static dictionary - keys initialised at

    """
    def __init__(self, keys, default=None):
        self._dict = dict.fromkeys(keys, default)

    def __getitem__(self, key):
        return self._dict[key]

    def __setitem__(self, key, value):
        if key not in self._dict.keys():
            raise KeyError(f'No key {key!r} for object of type {self.__class__.__name__!r}')
        self._dict[key] = value

    def __delitem__(self, key):
        raise TypeError(f'Objects of type {self.__class__.__name__!r} do not support deletion.')

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return len(self._dict)

    def __repr__(self):
        return self._dict.__repr__()