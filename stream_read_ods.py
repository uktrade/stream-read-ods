from collections import namedtuple
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import re

from lxml import etree
from stream_unzip import UnzipValueError, stream_unzip


def stream_read_ods(ods_chunks, max_string_length=65536, max_columns=65536, chunk_size=65536):

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

        def _next(it):
            try:
                return next(it)
            except etree.LxmlError as e:
                raise InvalidContentXMLError() from e

        def _append(l, value):
            if len(l) == max_columns:
                raise TooManyColumnsError(max_columns)
            l.append(value)

        def table_rows(parsed_xml_it):
            row = None

            while True:
                event, element = _next(parsed_xml_it)

                # Starting a row
                if event == 'start' and f'{ns_table}table-row' == element.tag:
                    row = []

                # Ending a row
                if event == 'end' and f'{ns_table}table-row' == element.tag:
                    yield tuple(row)

                # Starting a table cell
                if event == 'start' and f'{ns_table}table-cell' == element.tag:
                    _append(row, table_cell(parsed_xml_it, element))

                # Ending the table
                if event == 'end' and f'{ns_table}table' == element.tag:
                    return

                clear_mem(event, element)

        def table_cell(parsed_xml_it, cell_element):
            value_type = cell_element.attrib.get(f'{ns_office}value-type')
            return \
                None if value_type is None else \
                parse_boolean(cell_element) if value_type == 'boolean' else \
                parse_currency(cell_element) if value_type == 'currency' else \
                parse_date(cell_element) if value_type == 'date' else \
                parse_float(cell_element) if value_type == 'float' else \
                parse_percentage(cell_element) if value_type == 'percentage' else \
                parse_string(cell_element, parsed_xml_it) if value_type == 'string' else \
                parse_time(cell_element) if value_type == 'time' else \
                error(InvalidTypeError(value_type))

        def error(e):
            raise e(message)

        def parse_boolean(cell_element):
            value = cell_element.attrib[f'{ns_office}boolean-value']
            return \
                True if value == 'true' else \
                False if value == 'false' else \
                error(InvalidBooleanValueError(value))

        def parse_currency(cell_element):
            value = cell_element.attrib[f'{ns_office}value']
            try:
                return Currency(value, code=cell_element.attrib.get(f'{ns_office}currency'))
            except InvalidOperation as e:
                raise InvalidCurrencyValueError(value) from e

        def parse_date(cell_element):
            value = cell_element.attrib[f'{ns_office}date-value']
            try:
                try:
                    return date.fromisoformat(value)
                except ValueError:
                    return datetime.fromisoformat(value)
            except ValueError as e:
                raise InvalidDateValueError(value) from e

        def parse_float(cell_element):
            value = cell_element.attrib[f'{ns_office}value']
            try:
                return Decimal(value)
            except InvalidOperation as e:
                raise InvalidFloatValueError(value) from e

        def parse_percentage(cell_element):
            value = cell_element.attrib[f'{ns_office}value']
            try:
                return Percentage(value)
            except InvalidOperation as e:
                raise InvalidPercentageValueError(e) from e

        def parse_string(cell_element, parsed_xml_it):

            def itertext():
                # Like lxml's itertext, but doesn't use recursion and clears memory along the way
                l = 0
                previous_event = None
                text = ''
                while True:
                    event, element = _next(parsed_xml_it)
                    if event == 'end':
                        element_text = (element.text or '')
                        element_tail = (element.tail or '') if element is not cell_element else ''

                        l += len(element_text) + len(element_tail)
                        if l > max_string_length:
                            raise StringTooLongError(max_string_length)
                        text = \
                            (element_text + text + element_tail) if previous_event == 'end' else \
                            (text + element_text + element_tail)
                        clear_mem(event, element)

                    if event == 'end' and element is cell_element:
                        break

                    previous_event = event

                return text

            attribute_string_value = cell_element.attrib.get(f'{ns_office}:string-value')
            return \
                attribute_string_value if attribute_string_value is not None else \
                itertext()

        def parse_time(cell_element):
            value = cell_element.attrib[f'{ns_office}time-value']
            try:
                time_dict = time_regex.match(value).groupdict(0)
            except AttributeError as e:
                raise InvalidTimeValueError(value) from e

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
                event, element = _next(parsed_xml_it)
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
    content_xml_parsed = etree.iterparse(content_xml_file_like_obj, events=('start', 'end'), resolve_entities=False)

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


class InvalidContentXMLError(InvalidODSFileError):
    pass


class InvalidODSXMLError(InvalidODSFileError):
    pass


class InvalidTypeError(InvalidODSXMLError):
    pass


class InvalidValueError(InvalidODSXMLError):
    pass


class InvalidBooleanValueError(InvalidValueError):
    pass


class InvalidDateValueError(InvalidValueError):
    pass


class InvalidFloatValueError(InvalidValueError):
    pass


class InvalidPercentageValueError(InvalidValueError):
    pass


class InvalidTimeValueError(InvalidValueError):
    pass


class SizeError(StreamReadODSError):
    pass


class TooManyColumnsError(StreamReadODSError):
    pass


class StringTooLongError(StreamReadODSError):
    pass
