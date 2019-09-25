import zipfile
from bs4 import BeautifulSoup

import _utils

class PivotCache(object):
    def __init__(self, file_name=None):
        self.pivot_cache_name = None
        self.file_name = file_name

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

    def parse(self, file_name):
        self.pivot_cache_name = 'pivotCacheDefinition'
        self.file_name = file_name
        xmls = self.read()
        soups = [BeautifulSoup(xml, "xml") for xml in xmls]
        columns_metadatas = [soup.findAll("cacheField") for soup in soups]
        return [map(_utils.parse_column_metadata, columns_metadata)
                for columns_metadata in columns_metadatas]


class PivotCacheRecords(PivotCache):
    def __init__(self, file_name):
        super(self.__class__, self).__init__()
        self.pivot_cache_name = 'pivotCacheRecords'
        self.file_name = file_name
