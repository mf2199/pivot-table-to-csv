import logging
import progressbar
from multiprocessing import Manager, Process
from optparse import OptionParser

from pivot_cache import PivotCache
import _utils

TEST_FILE = r'C:\hartree\files\argentina\td_cargas_y_subproductos_obtenidos.xlsx'

def _parse_console_input():
    """ Console input parser

    :return:
    """
    parser = OptionParser()
    parser.add_option("-f", "--file", dest="filename",
                      help="Path to input file", metavar="FILE")
    parser.add_option("-o", "--output", dest="outputname",
                      help="Number of pieces to split before converting the file",
                      metavar="FILE")
    parser.add_option("-n", "--nchunks", dest="nchunks", default=5, type="int",
                      help="Path to output file", metavar="FILE")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                      help="Log in console", metavar="FILE")
    (options, _) = parser.parse_args()

    if not options.filename:
        # parser.error("Filename not given")
        logging.warning('Filename is not given. Trying default test file.')
        options.filename = TEST_FILE
    if options.verbose:
        logging.basicConfig(level=logging.DEBUG)
    if not options.outputname:
        options.outputname = '{}.csv'.format(options.filename.split('.')[0])

    return options.filename, options.outputname, options.nchunks


if __name__ == "__main__":
    file_name, output_file, n_chunks = _parse_console_input()

    logging.info('Extracting pivotCacheRecords from {}..'.format(file_name))
    pivot_cache = PivotCache(file_name=file_name)
    records = pivot_cache.get_records()
    column_metadata = pivot_cache.get_column_metadata()

    progress_bar = progressbar.ProgressBar(max_value=len(records) * n_chunks + 5)
    for i, xml, metadata in zip(range(1, len(records) + 1), records, column_metadata):
        progress_bar.update(0)
        batch_string = Manager().list()

        logging.info('Extracting metadata from pivotCacheDefinition')
        metadata = list(metadata)
        logging.debug(metadata)

        header = ','.join([column_data["column_name"] for column_data in metadata])
        batch_string.append(header + '\n')
        logging.debug(header)

        logging.info('Splitting pivotCacheRecords{}.xml into {} chunks'.format(i, n_chunks))
        chunks = _utils.split_xml(xml, n_chunks)

        for j in range(len(chunks)):
            logging.info('Converting chunk {} of pivotCacheRecords{}.csv'.format(j, i))
            valid_xml = _utils.get_valid_pivot_cache_records_xml(chunks, j)

            logging.debug('Chunk head {}'.format(valid_xml[:200]))
            logging.debug('Chunk tail {}'.format(valid_xml[-200:]))
            p = Process(target=_utils.str_xml_to_csv,
                        args=(valid_xml, batch_string, metadata,))
            p.start()
            p.join()
            logging.info(
                'Chunk {} of pivotCacheRecords{}.csv has been successfully converted'
                    .format(j, i))
            progress_bar.update(int((j + 1) * i))

        logging.info('Saving result in {}...'.format(output_file))
        _utils.write_csv(output_file + '-' + str(i), batch_string)
        logging.info('CSV file {} has been successfully created'.format(output_file))
