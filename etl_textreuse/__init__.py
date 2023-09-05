from dagster import Definitions, load_assets_from_modules, load_assets_from_package_module

from . import assets

# from .assets import raw_textreuses

all_assets = load_assets_from_package_module(package_module=assets)
# all_assets = load_assets_from_modules([raw_textreuses])

defs = Definitions(
    assets=all_assets,
)