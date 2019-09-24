import zipfile
from bs4 import BeautifulSoup

import _utils


class PivotCache(object):
	def __init__(self, file_name):
		self._file_name = file_name
		self._xml_records = None
		self._columns_metadata = []

	def _open(self):
		xlsx_file = zipfile.ZipFile(self._file_name)
		cache_names = list(filter(
			lambda x:'pivotCacheRecords' in x and x[-3:] == "xml",
			xlsx_file.namelist()))
		cache_names = sorted(cache_names)
		if len(cache_names) == 0:
			raise Exception('No valid Pivot Cache Records found')
		return [xlsx_file.open(cache_name) for cache_name in cache_names]

	def _parse_column_metadata(self, column_metadata):
		""" Column metadata parser

		:param column_metadata:
		:return:
		"""
		levels_tags = column_metadata.find("sharedItems").findAll()
		levels = [_utils.cast_tag_value(item.name, item.get("v", "")) for item in levels_tags]

		return {
			"column_name": _utils.cast_tag_value("s", column_metadata["name"]),
			"is_categorical": len(levels) > 0,
			"levels": levels
		}

	def get_records(self):
		self._xml_records = [
			str(cache_file.read()).encode("utf-8") for cache_file in self._open()
		]
		return self._xml_records

	def get_column_metadata(self):
		if self._xml_records is None:
			self.get_records()
		soups = [BeautifulSoup(xml, "xml") for xml in self._xml_records]
		column_metadata = [soup.findAll("cacheField") for soup in soups]
		return [map(self._parse_column_metadata, metadata) for metadata in column_metadata]
