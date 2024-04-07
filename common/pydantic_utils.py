from datetime import datetime
from typing import Optional, Callable


def str_validator(trim=False, empty_to_none=False, allow_empty=True) -> Callable[[Optional[str]], Optional[str]]:
    def _check(v: Optional[str]) -> Optional[str]:
        v = v.strip() if trim and v is not None else v
        v = None if empty_to_none and v == "" else v
        if not allow_empty and (v == "" or v is None):
            raise ValueError("String can't be empty")
        return v

    return _check


def pos_num_check(value: float | int) -> float | int:
    if value < 0:
        raise ValueError("Number must be positive")
    return value


