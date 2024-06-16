"""
Utilities for extensions of and operations on Python collections.
"""

import io
import itertools
import types
import warnings
from collections import OrderedDict, defaultdict
from collections.abc import Iterator as IteratorABC
from collections.abc import Sequence
from dataclasses import fields, is_dataclass
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Hashable,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)
from unittest.mock import Mock

import pydantic

# Quote moved to `prefect.utilities.annotations` but preserved here for compatibility
from prefect.utilities.annotations import BaseAnnotation, Quote, quote  # noqa


class AutoEnum(str, Enum):
    """
    An enum class that automatically generates value from variable names.

    This guards against common errors where variable names are updated but values are
    not.

    In addition, because AutoEnums inherit from `str`, they are automatically
    JSON-serializable.

    See https://docs.python.org/3/library/enum.html#using-automatic-values

    Example:
        ```python
        class MyEnum(AutoEnum):
            RED = AutoEnum.auto() # equivalent to RED = 'RED'
            BLUE = AutoEnum.auto() # equivalent to BLUE = 'BLUE'
        ```
    """

    def _generate_next_value_(name, start, count, last_values):
        return name

    @staticmethod
    def auto():
        """
        Exposes `enum.auto()` to avoid requiring a second import to use `AutoEnum`
        """
        return auto()

    def __repr__(self) -> str:
        return f"{type(self).__name__}.{self.value}"


KT = TypeVar("KT")
VT = TypeVar("VT")


def dict_to_flatdict(
    dct: Dict[KT, Union[Any, Dict[KT, Any]]], _parent: Tuple[KT, ...] = None
) -> Dict[Tuple[KT, ...], Any]:
    """Converts a (nested) dictionary to a flattened representation.

    Each key of the flat dict will be a CompoundKey tuple containing the "chain of keys"
    for the corresponding value.

    Args:
        dct (dict): The dictionary to flatten
        _parent (Tuple, optional): The current parent for recursion

    Returns:
        A flattened dict of the same type as dct
    """
    typ = cast(Type[Dict[Tuple[KT, ...], Any]], type(dct))
    items: List[Tuple[Tuple[KT, ...], Any]] = []
    parent = _parent or tuple()

    for k, v in dct.items():
        k_parent = tuple(parent + (k,))
        # if v is a non-empty dict, recurse
        if isinstance(v, dict) and v:
            items.extend(dict_to_flatdict(v, _parent=k_parent).items())
        else:
            items.append((k_parent, v))
    return typ(items)


def flatdict_to_dict(
    dct: Dict[Tuple[KT, ...], VT],
) -> Dict[KT, Union[VT, Dict[KT, VT]]]:
    """Converts a flattened dictionary back to a nested dictionary.

    Args:
        dct (dict): The dictionary to be nested. Each key should be a tuple of keys
            as generated by `dict_to_flatdict`

    Returns
        A nested dict of the same type as dct
    """
    typ = type(dct)
    result = cast(Dict[KT, Union[VT, Dict[KT, VT]]], typ())
    for key_tuple, value in dct.items():
        current_dict = result
        for prefix_key in key_tuple[:-1]:
            # Build nested dictionaries up for the current key tuple
            # Use `setdefault` in case the nested dict has already been created
            current_dict = current_dict.setdefault(prefix_key, typ())  # type: ignore
        # Set the value
        current_dict[key_tuple[-1]] = value

    return result


T = TypeVar("T")


def isiterable(obj: Any) -> bool:
    """
    Return a boolean indicating if an object is iterable.

    Excludes types that are iterable but typically used as singletons:
    - str
    - bytes
    - IO objects
    """
    try:
        iter(obj)
    except TypeError:
        return False
    else:
        return not isinstance(obj, (str, bytes, io.IOBase))


def ensure_iterable(obj: Union[T, Iterable[T]]) -> Iterable[T]:
    if isinstance(obj, Sequence) or isinstance(obj, Set):
        return obj
    obj = cast(T, obj)  # No longer in the iterable case
    return [obj]


def listrepr(objs: Iterable[Any], sep: str = " ") -> str:
    return sep.join(repr(obj) for obj in objs)


def extract_instances(
    objects: Iterable,
    types: Union[Type[T], Tuple[Type[T], ...]] = object,
) -> Union[List[T], Dict[Type[T], T]]:
    """
    Extract objects from a file and returns a dict of type -> instances

    Args:
        objects: An iterable of objects
        types: A type or tuple of types to extract, defaults to all objects

    Returns:
        If a single type is given: a list of instances of that type
        If a tuple of types is given: a mapping of type to a list of instances
    """
    types = ensure_iterable(types)

    # Create a mapping of type -> instance from the exec values
    ret = defaultdict(list)

    for o in objects:
        # We iterate here so that the key is the passed type rather than type(o)
        for type_ in types:
            if isinstance(o, type_):
                ret[type_].append(o)

    if len(types) == 1:
        return ret[types[0]]

    return ret


def batched_iterable(iterable: Iterable[T], size: int) -> Iterator[Tuple[T, ...]]:
    """
    Yield batches of a certain size from an iterable

    Args:
        iterable (Iterable): An iterable
        size (int): The batch size to return

    Yields:
        tuple: A batch of the iterable
    """
    it = iter(iterable)
    while True:
        batch = tuple(itertools.islice(it, size))
        if not batch:
            break
        yield batch


class StopVisiting(BaseException):
    """
    A special exception used to stop recursive visits in `visit_collection`.

    When raised, the expression is returned without modification and recursive visits
    in that path will end.
    """


def visit_collection(
    expr: Any,
    visit_fn: Union[Callable[[Any, Optional[dict]], Any], Callable[[Any], Any]],
    return_data: bool = False,
    max_depth: int = -1,
    context: Optional[dict] = None,
    remove_annotations: bool = False,
    _seen: Optional[Set[int]] = None,
) -> Any:
    """
    Visits and potentially transforms every element of an arbitrary Python collection.

    If an element is a Python collection, it will be visited recursively. If an element
    is not a collection, `visit_fn` will be called with the element. The return value of
    `visit_fn` can be used to alter the element if `return_data` is set to `True`.

    Note:
    - When `return_data` is `True`, a copy of each collection is created only if
      `visit_fn` modifies an element within that collection. This approach minimizes
      performance penalties by avoiding unnecessary copying.
    - When `return_data` is `False`, no copies are created, and only side effects from
      `visit_fn` are applied. This mode is faster and should be used when no transformation
      of the collection is required, because it never has to copy any data.

    Supported types:
    - List (including iterators)
    - Tuple
    - Set
    - Dict (note: keys are also visited recursively)
    - Dataclass
    - Pydantic model
    - Prefect annotations

    Note that visit_collection will not consume generators or async generators, as it would prevent
    the caller from iterating over them.

    Args:
        expr (Any): A Python object or expression.
        visit_fn (Callable[[Any, Optional[dict]], Any] or Callable[[Any], Any]): A function
            that will be applied to every non-collection element of `expr`. The function can
            accept one or two arguments. If two arguments are accepted, the second argument
            will be the context dictionary.
        return_data (bool): If `True`, a copy of `expr` containing data modified by `visit_fn`
            will be returned. This is slower than `return_data=False` (the default).
        max_depth (int): Controls the depth of recursive visitation. If set to zero, no
            recursion will occur. If set to a positive integer `N`, visitation will only
            descend to `N` layers deep. If set to any negative integer, no limit will be
            enforced and recursion will continue until terminal items are reached. By
            default, recursion is unlimited.
        context (Optional[dict]): An optional dictionary. If passed, the context will be sent
            to each call to the `visit_fn`. The context can be mutated by each visitor and
            will be available for later visits to expressions at the given depth. Values
            will not be available "up" a level from a given expression.
            The context will be automatically populated with an 'annotation' key when
            visiting collections within a `BaseAnnotation` type. This requires the caller to
            pass `context={}` and will not be activated by default.
        remove_annotations (bool): If set, annotations will be replaced by their contents. By
            default, annotations are preserved but their contents are visited.
        _seen (Optional[Set[int]]): A set of object ids that have already been visited. This
            prevents infinite recursion when visiting recursive data structures.

    Returns:
        Any: The modified collection if `return_data` is `True`, otherwise `None`.
    """

    if _seen is None:
        _seen = set()

    def visit_nested(expr):
        # Utility for a recursive call, preserving options and updating the depth.
        return visit_collection(
            expr,
            visit_fn=visit_fn,
            return_data=return_data,
            remove_annotations=remove_annotations,
            max_depth=max_depth - 1,
            # Copy the context on nested calls so it does not "propagate up"
            context=context.copy() if context is not None else None,
            _seen=_seen,
        )

    def visit_expression(expr):
        if context is not None:
            return visit_fn(expr, context)
        else:
            return visit_fn(expr)

    if id(expr) in _seen:
        # If we have already visited this expression, do not visit it again
        return expr if return_data else None

    # Visit every expression
    try:
        result = visit_expression(expr)
        _seen.add(id(expr))
    except StopVisiting:
        max_depth = 0
        result = expr

    if return_data:
        # Only mutate the root expression if the user indicated we're returning data,
        # otherwise the function could return null and we have no collection to check
        expr = result

    # Then, visit every child of the expression recursively

    # If we have reached the maximum depth, do not perform any recursion
    if max_depth == 0:
        return result if return_data else None

    # Get the expression type; treat iterators like lists
    typ = list if isinstance(expr, IteratorABC) and isiterable(expr) else type(expr)
    typ = cast(type, typ)  # mypy treats this as 'object' otherwise and complains

    # Then visit every item in the expression if it is a collection

    # presume that the result is the original expression.
    # in each of the following cases, we will update the result if we need to.
    result = expr

    # --- Generators

    if isinstance(expr, (types.GeneratorType, types.AsyncGeneratorType)):
        # Do not attempt to iterate over generators, as it will exhaust them
        pass

    # --- Mocks

    elif isinstance(expr, Mock):
        # Do not attempt to recurse into mock objects
        pass

    # --- Annotations (unmapped, quote, etc.)

    elif isinstance(expr, BaseAnnotation):
        if context is not None:
            context["annotation"] = expr
        unwrapped = expr.unwrap()
        value = visit_nested(unwrapped)

        if return_data:
            # if we are removing annotations, return the value
            if remove_annotations:
                result = value
            # if the value was modified, rewrap it
            elif value is not unwrapped:
                result = expr.rewrap(value)
            # otherwise return the expr

    # --- Sequences

    elif typ in (list, tuple, set):
        items = [visit_nested(o) for o in expr]
        if return_data:
            modified = any(item is not orig for item, orig in zip(items, expr))
            if modified:
                result = typ(items)

    # --- Dictionaries

    elif typ in (dict, OrderedDict):
        assert isinstance(expr, (dict, OrderedDict))  # typecheck assertion
        items = [(visit_nested(k), visit_nested(v)) for k, v in expr.items()]
        if return_data:
            modified = any(
                k1 is not k2 or v1 is not v2
                for (k1, v1), (k2, v2) in zip(items, expr.items())
            )
            if modified:
                result = typ(items)

    # --- Dataclasses

    elif is_dataclass(expr) and not isinstance(expr, type):
        values = [visit_nested(getattr(expr, f.name)) for f in fields(expr)]
        if return_data:
            modified = any(
                getattr(expr, f.name) is not v for f, v in zip(fields(expr), values)
            )
            if modified:
                result = typ(**{f.name: v for f, v in zip(fields(expr), values)})

    # --- Pydantic models

    elif isinstance(expr, pydantic.BaseModel):
        typ = cast(Type[pydantic.BaseModel], typ)

        # when extra=allow, fields not in model_fields may be in model_fields_set
        model_fields = expr.model_fields_set.union(expr.model_fields.keys())

        # We may encounter a deprecated field here, but this isn't the caller's fault
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=DeprecationWarning)

            updated_data = {
                field: visit_nested(getattr(expr, field)) for field in model_fields
            }

        if return_data:
            modified = any(
                getattr(expr, field) is not updated_data[field]
                for field in model_fields
            )
            if modified:
                # Use construct to avoid validation and handle immutability
                model_instance = typ.model_construct(
                    _fields_set=expr.model_fields_set, **updated_data
                )
                for private_attr in expr.__private_attributes__:
                    setattr(model_instance, private_attr, getattr(expr, private_attr))
                result = model_instance

    if return_data:
        return result


def remove_nested_keys(keys_to_remove: List[Hashable], obj):
    """
    Recurses a dictionary returns a copy without all keys that match an entry in
    `key_to_remove`. Return `obj` unchanged if not a dictionary.

    Args:
        keys_to_remove: A list of keys to remove from obj obj: The object to remove keys
            from.

    Returns:
        `obj` without keys matching an entry in `keys_to_remove` if `obj` is a
            dictionary. `obj` if `obj` is not a dictionary.
    """
    if not isinstance(obj, dict):
        return obj
    return {
        key: remove_nested_keys(keys_to_remove, value)
        for key, value in obj.items()
        if key not in keys_to_remove
    }


def distinct(
    iterable: Iterable[T],
    key: Callable[[T], Any] = (lambda i: i),
) -> Generator[T, None, None]:
    seen: Set = set()
    for item in iterable:
        if key(item) in seen:
            continue
        seen.add(key(item))
        yield item


def get_from_dict(dct: Dict, keys: Union[str, List[str]], default: Any = None) -> Any:
    """
    Fetch a value from a nested dictionary or list using a sequence of keys.

    This function allows to fetch a value from a deeply nested structure
    of dictionaries and lists using either a dot-separated string or a list
    of keys. If a requested key does not exist, the function returns the
    provided default value.

    Args:
        dct: The nested dictionary or list from which to fetch the value.
        keys: The sequence of keys to use for access. Can be a
            dot-separated string or a list of keys. List indices can be included
            in the sequence as either integer keys or as string indices in square
            brackets.
        default: The default value to return if the requested key path does not
            exist. Defaults to None.

    Returns:
        The fetched value if the key exists, or the default value if it does not.

    Examples:
    >>> get_from_dict({'a': {'b': {'c': [1, 2, 3, 4]}}}, 'a.b.c[1]')
    2
    >>> get_from_dict({'a': {'b': [0, {'c': [1, 2]}]}}, ['a', 'b', 1, 'c', 1])
    2
    >>> get_from_dict({'a': {'b': [0, {'c': [1, 2]}]}}, 'a.b.1.c.2', 'default')
    'default'
    """
    if isinstance(keys, str):
        keys = keys.replace("[", ".").replace("]", "").split(".")
    try:
        for key in keys:
            try:
                # Try to cast to int to handle list indices
                key = int(key)
            except ValueError:
                # If it's not an int, use the key as-is
                # for dict lookup
                pass
            dct = dct[key]
        return dct
    except (TypeError, KeyError, IndexError):
        return default
