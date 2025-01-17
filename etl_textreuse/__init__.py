from dagster import Definitions, load_assets_from_modules, load_assets_from_package_module, in_process_executor

from . import assets,database_assets,additional_assets

# from .assets import raw_textreuses

all_spark_assets = load_assets_from_package_module(package_module=assets)
all_db_assets = load_assets_from_package_module(package_module=database_assets)
all_additional_assets = load_assets_from_package_module(package_module=additional_assets)
# all_assets = load_assets_from_modules([raw_textreuses])

defs = Definitions(
    assets=[*all_spark_assets,*all_db_assets,*all_additional_assets],executor=in_process_executor
)