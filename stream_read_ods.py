from collections import namedtuple
from datetime import date, datetime
from decimal import Decimal
import re

from lxml import etree
from stream_unzip import UnzipValueError, stream_unzip


def stream_read_ods(ods_chunks, chunk_size=65536):

    # lxml iterparse takes a file-like object, but stream_read_ods accepts an iterable
    # so we have to do some low-ish level faffing to convert from one to the other
    def to_file_like_obj(bytes_iter):
        chunk = b''
        offset = 0
        it = iter(bytes_iter)

        def up_to_iter(size):
            nonlocal chunk, offset

            while size:
                if offset == len(chunk):
                    try:
                        chunk = next(it)
                    except StopIteration:
                        break
                    else:
                        offset = 0
                to_yield = min(size, len(chunk) - offset)
                offset = offset + to_yield
                size -= to_yield
                yield chunk[offset - to_yield:offset]

        class FileLikeObj:
            def read(self, size=-1):
                return b''.join(up_to_iter(float('inf') if size is None or size < 0 else size))

        return FileLikeObj()

    def validate_mimetype_and_get_content(unzipped_files):
        correct_mimetype = b'application/vnd.oasis.opendocument.spreadsheet'

        for i, (name, size, chunks) in enumerate(unzipped_files):
            if i == 0 and name != b'mimetype':
                raise MissingMIMETypeError()

            if i == 0 and name == b'mimetype':
                mimetype = b''
                chunks_it = iter(chunks)
                while len(mimetype) < len(correct_mimetype):
                    try:
                        mimetype += next(chunks_it)
                    except StopIteration:
                        break
                if mimetype != correct_mimetype:
                    raise IncorrectMIMETypeError(mimetype.decode("utf-8"))

            if name != b'content.xml':
                for chunk in chunks:
                    pass
                continue

            yield from chunks
            break
        else:
            raise MissingContentXMLError()

    def get_sheets_and_rows(parsed_xml):

        # Thanks to https://stackoverflow.com/a/2765366/1319998
        time_regex = re.compile(r'(?P<sign>-?)P(?:(?P<years>\d+)Y)?(?:(?P<months>\d+)M)?(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>([0-9]*[.])?[0-9]+)S)?)?')
        ns_table = '{urn:oasis:names:tc:opendocument:xmlns:table:1.0}'
        ns_office = '{urn:oasis:names:tc:opendocument:xmlns:office:1.0}'

        def table_rows(parsed_xml_it):
            row = None

            while True:
                event, element = next(parsed_xml_it)

                # Starting a row
                if event == 'start' and f'{ns_table}table-row' == element.tag:
                    row = []

                # Ending a row
                if event == 'end' and f'{ns_table}table-row' == element.tag:
                    yield tuple(row)

                # Starting a table cell
                if event == 'start' and f'{ns_table}table-cell' == element.tag:
                    row.append(table_cell(parsed_xml_it, element))

                # Ending the table
                if event == 'end' and f'{ns_table}table' == element.tag:
                    return

                clear_mem(event, element)

        def table_cell(parsed_xml_it, cell_element):
            value_type = cell_element.attrib.get(f'{ns_office}value-type')

            # Non-strings are always from attributes
            if value_type != 'string':
                return \
                    None if value_type is None else \
                    parse_boolean(cell_element) if value_type == 'boolean' else \
                    parse_currency(cell_element) if value_type == 'currency' else \
                    parse_date(cell_element) if value_type == 'date' else \
                    parse_float(cell_element) if value_type == 'float' else \
                    parse_percentage(cell_element) if value_type == 'percentage' else \
                    parse_time(cell_element) if value_type == 'time' else \
                    value_error(value_type)

            # Strings can be from an attribute...
            attribute_string_value = cell_element.attrib.get(f'{ns_office}:string-value')
            if attribute_string_value is not None:
                return attribute_string_value

            # ... but otherwise extract from contents
            while True:
                event, element = next(parsed_xml_it)
                if event == 'end' and element is cell_element:
                    return ''.join(cell_element.itertext())

        def value_error(message):
            raise ValueError(message)

        def parse_boolean(cell_element):
            value = cell_element.attrib[f'{ns_office}boolean-value']
            return \
                True if value == 'true' else \
                False if value == 'false' else \
                value_error(value)

        def parse_currency(cell_element):
            return Currency(cell_element.attrib[f'{ns_office}value'], code=cell_element.attrib.get(f'{ns_office}currency'))

        def parse_date(cell_element):
            value = cell_element.attrib[f'{ns_office}date-value']
            try:
                return date.fromisoformat(value)
            except ValueError:
                return datetime.fromisoformat(value)

        def parse_float(cell_element):
            return Decimal(cell_element.attrib[f'{ns_office}value'])

        def parse_percentage(cell_element):
            return Percentage(cell_element.attrib[f'{ns_office}value'])

        def parse_time(cell_element):
            time_dict = time_regex.match(cell_element.attrib[f'{ns_office}time-value']).groupdict(0)
            return Time(
                time_dict.get('sign') or '+',
                int(time_dict.get('years', '0')),
                int(time_dict.get('months', '0')),
                int(time_dict.get('days', '0')),
                int(time_dict.get('hours', '0')),
                int(time_dict.get('minutes', '0')),
                Decimal(time_dict.get('seconds', '0')),
            )

        def clear_mem(event, element):
            if event == 'end':
                element.clear()
                while element.getprevious() is not None:
                    del element.getparent()[0]

        # We use manual iteration to be able to delegate iterating
        parsed_xml_it = iter(parsed_xml)

        while True:
            try:
                event, element = next(parsed_xml_it)
            except StopIteration:
                break

            # Starting a table
            if event == 'start' and f'{ns_table}table' == element.tag:
                sheet_name = element.attrib[f'{ns_table}name']
                rows = table_rows(parsed_xml_it)
                yield sheet_name, rows
                for _ in rows:
                    raise UnfinishedIterationError()

            clear_mem(event, element)

    unzipped_member_files = stream_unzip(ods_chunks, chunk_size=chunk_size)
    content_xml_chunks = validate_mimetype_and_get_content(unzipped_member_files)
    content_xml_file_like_obj = to_file_like_obj(content_xml_chunks)
    content_xml_parsed = etree.iterparse(content_xml_file_like_obj, events=('start', 'end'))

    try:
        yield from get_sheets_and_rows(content_xml_parsed)
    except UnzipValueError as e:
        raise UnzipError() from e


def simple_table(rows, skip_rows=0):
    def up_to_first_none(values):
        vals = []
        for value in values:
            if value is None:
                break
            vals.append(value)
        return tuple(vals)

    def remaining_rows(width):
        for row in rows:
            remaining = max(0, width - len(row))
            row = row[:width] + (None,) * remaining
            if all((val is None) for val in row):
                break
            yield row

    for i, row in enumerate(rows):
        if i == skip_rows:
            header_row = up_to_first_none(row)
            return header_row, remaining_rows(width=len(header_row))


class Percentage(Decimal):
    pass


class Currency(Decimal):
    def __new__(cls, value='0', context=None, code=None):
        instance = super().__new__(cls, value, context)
        instance.code = code
        return instance

    def __eq__(self, other):
        return \
            isinstance(other, self.__class__) \
            and super().__eq__(other) \
            and self.code == other.code


Time = namedtuple('Time', ('sign', 'years', 'months', 'days', 'hours', 'minutes', 'seconds'), defaults=('+', 0, 0, 0, 0, Decimal('0')))


class StreamReadODSError(Exception):
    pass


class InvalidOperationError(StreamReadODSError):
    pass


class UnfinishedIterationError(InvalidOperationError):
    pass


class InvalidODSFileError(StreamReadODSError, ValueError):
    pass


class UnzipError(InvalidODSFileError):
    pass


class MissingMIMETypeError(InvalidODSFileError):
    pass


class IncorrectMIMETypeError(InvalidODSFileError):
    pass


class MissingContentXMLError(InvalidODSFileError):
    pass
