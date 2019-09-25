import logging
from multiprocessing import Manager, Process
from optparse import OptionParser
import progressbar

import pivot_cache
import _utils


def _parse_console_input():
    parser = OptionParser()
    parser.add_option("-f", "--file", dest="filename",
                      help="Path to input file", metavar="FILE")
    parser.add_option("-o", "--output", dest="outputname",
                      help="Number of pieces to split before converting the file", metavar="FILE")
    parser.add_option("-n", "--nchunks", dest="nchunks", default=5, type="int",
                      help="Path to output file", metavar="FILE")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                      help="Log in console", metavar="FILE")
    (options, _) = parser.parse_args()

    if options.filename is None:
        parser.error("Filename not given")
    if options.verbose is True:
        logging.basicConfig(level=logging.DEBUG)
    if options.outputname is None:
        options.outputname = options.filename.split('.')[0] + ".csv"

    return options.filename, options.outputname, options.nchunks


if __name__ == "__main__":
    file_name, output_file, n_chunks = _parse_console_input()

    logging.info('Extracting pivotCacheRecords from {}..'.format(file_name))
    cache = pivot_cache.PivotCache(file_name)
    records = cache.read()
    metadatas = cache.parse()

    bar = progressbar.ProgressBar(max_value=len(records) * n_chunks)
    bar.update(0)
    for i, xml, metadata in zip(range(1, len(records) + 1), records, metadatas):
        batch_string = Manager().list()

        logging.info('Extracting metadata from pivotCacheDefinition')
        metadata = list(metadata)
        logging.debug(metadata)

        header = _utils.get_header(metadata)
        batch_string.append(header + '\n')
        logging.debug(header)

        logging.info("Splitting pivotCacheRecords%d.xml into %d chunks", i, n_chunks)
        chunks = _utils.split_xml(xml, n_chunks)

        for j in range(len(chunks)):
            logging.info("Converting chunk %d of pivotCacheRecords%d.csv", j, i)
            valid_xml = _utils.get_valid_pivot_cache_records_xml(chunks, j)

            logging.debug("Chunk head %s", valid_xml[:200])
            logging.debug("Chunk tail %s", valid_xml[-200:])
            p = Process(target=_utils.xml_to_csv,
                        args=(valid_xml, batch_string, metadata,))

            p.start()
            p.join()
            logging.info("Chunk %d of pivotCacheRecords%d.csv successfully converted", j, i)
            bar.update(int((j + 1) + (i - 1) * len(chunks)))

        logging.info("Saving result in %s...", output_file)
        _utils.write_csv(output_file + '-' + str(i), batch_string)
        logging.info("CSV File %s successfully created", output_file)
