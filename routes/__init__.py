import importlib
import inspect
import os
import pathlib

from litestar import Controller, Router

MODULE_PATH = pathlib.Path(__file__).parent
MODULE_NAME = __name__

route_handlers = []

for item in os.listdir(MODULE_PATH):
    item_path = MODULE_PATH / item

    # --- Case 1: .py file (top-level)
    if item_path.is_file() and item.endswith(".py") and item != "__init__.py":
        mod_name = f"{MODULE_NAME}.{item[:-3]}"
        mod = importlib.import_module(mod_name)

        for _, obj in inspect.getmembers(mod):
            if isinstance(obj, Router) or (
                inspect.isclass(obj) and issubclass(obj, Controller) and obj.__module__ == mod.__name__
            ):
                route_handlers.append(obj)

    # --- Case 2: submodule with __init__.py
    elif item_path.is_dir() and (item_path / "__init__.py").exists():
        submodule_name = f"{MODULE_NAME}.{item}"
        submod = importlib.import_module(submodule_name)

        for _, obj in inspect.getmembers(submod):
            if isinstance(obj, Router):
                route_handlers.append(obj)
