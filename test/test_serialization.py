import json

import serialization
from main import add, append_to_dict


def test_function_serialization():
    add_params = json.dumps({"a": 1, "b": 2})
    assert 3 == serialization.test_runloop_serialization(add_params, add)


def test_dict_serialization():
    add_params = json.dumps({"my_dict": {"inner": ["world"]}})
    assert ['world', 'hello'] == serialization.test_runloop_serialization(add_params, append_to_dict)
