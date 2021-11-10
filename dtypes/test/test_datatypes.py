import pytest

from dtypes import StaticDict


def construct_StaticDict(keys, default=None):
    return StaticDict(keys, default)


class TestStaticDict:

    @pytest.mark.parametrize("keys,default", [
        (['a', 'b', 'c'], None),
        (['a', 'b', 'c'], 3.0),
        (['a', 'b', 'c'], 's'),
    ])
    def test_construction(self, keys, default):

        sd = StaticDict(keys, default)

        assert sd.get(keys[0]) == default
        assert sd.get(keys[1]) == default
        assert sd.get(keys[2]) == default

    @pytest.mark.parametrize("keys,default,update_key,update_val", [
        (['a', 'b', 'c'], None,  'a', 'a'),
        (['a', 'b', 'c'], 5.0,   'b', -5.0),
        (['a', 'b', 'c'], 'str', 'c', 'new'),
    ])
    def test_update_success(self, keys, default, update_key, update_val):
        sd = StaticDict(keys, default)

        # Check pre update state of values.
        assert all([val == default for val in sd.values()])
        # Update
        sd.update(**{update_key: update_val})
        # Check post update
        assert (all([val == default for key, val in sd.items() if key != update_key]))
        assert sd.get(update_key) == update_val

    @pytest.mark.parametrize("keys,key_for_update,val_for_update", [
        (['a', 'b', 'c'], 'd', 3)
    ])
    def test_update_failure(self, keys, key_for_update, val_for_update):

        sd = StaticDict(keys)

        with pytest.raises(KeyError) as e_info:
            sd.update(key_for_update=val_for_update)

    def test_delete(self):
        keys = ['a']
        default = 4
        sd = StaticDict(keys, default)

        with pytest.raises(TypeError) as e_info:
            del sd[[k for k in keys]]

