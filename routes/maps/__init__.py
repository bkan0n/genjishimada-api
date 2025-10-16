import importlib
import inspect
import logging
import os
import pathlib

import litestar

log = logging.getLogger(__name__)

MODULE_PATH = pathlib.Path(__file__).parent
MODULE_NAME = __name__  # e.g., "myapp.routes.maps"

controller_classes = []

for file in os.listdir(MODULE_PATH):
    if file.endswith(".py") and file != "__init__.py":
        module_name = f"{MODULE_NAME}.{file[:-3]}"  # strip .py
        module = importlib.import_module(module_name)

        # Inspect members and collect Controller subclasses
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if issubclass(obj, litestar.Controller) and obj.__module__ == module.__name__:
                controller_classes.append(obj)


maps_router = litestar.Router(path="/maps", route_handlers=controller_classes)
