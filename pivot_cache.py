import logging
import zipfile
from bs4 import BeautifulSoup

import _utils

class PivotCache(object):
    def __init__(self, file_name=None):
        self.pivot_cache_name = None
        self.file_name = file_name
        self._xl_content = None

    def get_xml_items(self, filter_):
        if self._xl_content is None:
            self._xl_content = zipfile.ZipFile(self.file_name)
        xml_items = list(filter(
            lambda x: filter_ in x and x[-3:] == "xml",
            self._xl_content.namelist()))
        return sorted(xml_items)

    def open(self):
        # xlsx_file = zipfile.ZipFile(self.file_name)
        # cache_names = list(filter(
        #     lambda x: self.pivot_cache_name in x and x[-3:] == "xml",
        #     xlsx_file.namelist()))
        # cache_names = sorted(cache_names)
        cache_names = self.get_xml_items(self.pivot_cache_name)
        if len(cache_names) == 0:
            raise Exception(
                'Could not find any "{}" (not a PivotTable Excel file?)'
                    .format(self.pivot_cache_name))
        return [self._xl_content.open(cache_name) for cache_name in cache_names]

    def read(self, pivot_cache_name='pivotCacheRecords'):
        self.pivot_cache_name = pivot_cache_name
        return [str(cache_file.read(), "utf-8") for cache_file in self.open()]

    def parse(self):
        xmls = self.read('pivotCacheDefinition')
        soups = [BeautifulSoup(xml, "xml") for xml in xmls]
        columns_metadatas = [soup.findAll("cacheField") for soup in soups]
        return [map(_utils.parse_column_metadata, columns_metadata)
                for columns_metadata in columns_metadatas]
