""" Validate Runloop serialization. """
import base64
import json
from typing import get_origin, get_args, Type, Any

import runloop
from pydantic import BaseModel


def parse_type_from_json(json_element, expected_type: Type) -> any:
    """Coalesce `expected_type` type from JSON representation of that type."""
    origin = get_origin(expected_type)
    if origin is None and issubclass(expected_type, BaseModel):
        return expected_type.model_validate(json_element)
    elif expected_type is bytes:
        return base64.b64decode(json_element)
    elif origin is list:
        args = get_args(expected_type)
        return [parse_type_from_json(x, args[0]) for x in json_element]
    elif origin is dict:
        args = get_args(expected_type)
        return {k: parse_type_from_json(v, args[1]) for k, v in json_element.items()}
    else:
        return json_element


def parse_function_kwargs(descriptor: runloop.FunctionDescriptor,
                          json_payload: str,
                          ) -> (dict, runloop.Session):
    """Parse the request payload string as a JSON object and map it to the target function's arguments.
    If the user has registered a `Session` as a parameter, parse the session metadata available in the request.
    """
    kwargs = {}
    request_json = json.loads(json_payload)

    # Try to match args to request
    for parameter in descriptor.parameters:
        expected_type = parameter.type

        # If not session, use runloop type manifest to parse the request
        data = request_json.get(parameter.name)
        if data is None:
            raise ValueError(f"Missing parameter {parameter.name} in request payload")
        kwargs[parameter.name] = parse_type_from_json(data, expected_type.annotation)

    return kwargs


def test_runloop_serialization(request_payload: str, wrapper_function: runloop.WrappedFunction) -> Any:
    kwargs = parse_function_kwargs(wrapper_function.descriptor, request_payload)
    invocation = wrapper_function(**kwargs)
    return invocation.invoke()


def invoke_with_json():
    pass
