import zipfile
from bs4 import BeautifulSoup
from utils import cast_tag_value


class PivotCache:
    _pivot_cache_name = None
    _file_name = None

    def open(self):
        xlsx_file = zipfile.ZipFile(self.file_name)
        cache_names = list(filter(lambda x: self.pivot_cache_name in x and x[-3:] == "xml", xlsx_file.namelist()))
        cache_names = sorted(cache_names)
        if len(cache_names) == 0:
            error_message = "Could not find any {0}: It is not a PivotTable excel file"
            raise Exception(error_message.format(self.pivot_cache_name))
        return [xlsx_file.open(cache_name) for cache_name in cache_names]

    def read(self):
        return [str(cache_file.read(), "utf-8") for cache_file in self.open()]

    @property
    def pivot_cache_name(self):
        if self._pivot_cache_name is None:
            raise NotImplementedError
        else:
            return self._pivot_cache_name

    @pivot_cache_name.setter
    def pivot_cache_name(self, value):
        self._pivot_cache_name = value

    @property
    def file_name(self):
        if self._file_name is None:
            raise NotImplementedError
        else:
            return self._file_name

    @file_name.setter
    def file_name(self, value):
        self._file_name = value


class PivotCacheRecords(PivotCache):
    def __init__(self, file_name):
        self.pivot_cache_name = 'pivotCacheRecords'
        self.file_name = file_name


class PivotCacheDefinition(PivotCache):
    def __init__(self, file_name):
        self.pivot_cache_name = 'pivotCacheDefinition'
        self.file_name = file_name

    def parse(self):
        xmls = self.read()
        soups = [BeautifulSoup(xml, "xml") for xml in xmls]
        columns_metadatas = [soup.findAll("cacheField") for soup in soups]
        return [map(PivotCacheDefinition.parse_column_metadata, columns_metadata)
                for columns_metadata in columns_metadatas]

    @staticmethod
    def parse_column_metadata(column_metadata):
        levels = PivotCacheDefinition.parse_shared_items(column_metadata.find("sharedItems"))
        return {
            "column_name": cast_tag_value("s", column_metadata["name"]),
            "is_categorical": len(levels) > 0,
            "levels": levels
        }

    @staticmethod
    def parse_shared_items(shared_items):
        levels_tags = []
        if shared_items is not None:
            levels_tags = shared_items.findAll()
        return [cast_tag_value(item.name, item.get("v", "")) for item in levels_tags]
