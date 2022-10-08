from datetime import date, datetime
from itertools import groupby

from lxml import etree
from stream_unzip import stream_unzip


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

    def get_member_file(single_file_name, unzipped_files):
        for name, size, chunks in unzipped_files:
            if name != single_file_name:
                for chunk in chunks:
                    pass
                continue
            yield from chunks

    def get_sheets_and_rows(parsed_xml):
        # It's slightly tricky to convert the event-based lxml parser to more useful
        # "nested iterable" API. To do this, this generator yields tuples of
        #
        # (sheet, sheet_name, row)
        #
        # where sheet is a plain object() used as a sentinal value so a later call to
        # groupby can then group rows into sheets
        #
        # The name of the sheet is not used since sheets can have identical names

        def value_error():
            raise ValueError()

        def parse_float(value):
            try:
                return int(value)
            except ValueError:
                return float(value)

        def parse_boolean(value):
            return \
                True if value == 'true' else \
                False if value == 'false' else \
                value_error()

        def parse_date(value):
            try:
                return date.fromisoformat(value)
            except ValueError:
                return datetime.fromisoformat(value)

        ns_table = '{urn:oasis:names:tc:opendocument:xmlns:table:1.0}'
        ns_office = '{urn:oasis:names:tc:opendocument:xmlns:office:1.0}'

        # A sentinal per sheet, to be able to groupby
        sheet = None

        # The name of the sheet (exposed in the API)
        sheet_name = None

        # We use manual iteration to be able to delegate iterating
        parsed_xml_it = iter(parsed_xml)

        while True:
            try:
                event, element = next(parsed_xml_it)
            except StopIteration:
                break

            # Starting a table
            if event == 'start' and f'{ns_table}table' == element.tag:
                sheet = object()
                sheet_name = element.attrib[f'{ns_table}name']

            # Starting a row
            if event == 'start' and f'{ns_table}table-row' == element.tag:
                row = []

            # Ending a row
            if event == 'end' and f'{ns_table}table-row' == element.tag:
                yield sheet, sheet_name, tuple(row)

            # Starting a table cell
            if event == 'start' and f'{ns_table}table-cell' == element.tag:
                value_type = element.attrib.get(f'{ns_office}value-type')
                row.append(
                    None if value_type is None else \
                    parse_boolean(element.attrib[f'{ns_office}boolean-value']) if value_type == 'boolean' else \
                    parse_date(element.attrib[f'{ns_office}date-value']) if value_type == 'date' else \
                    parse_float(element.attrib[f'{ns_office}value']) if value_type == 'float' else \
                    '' if value_type == 'string' else \
                    value_error()
                )

            # Reduce memory usage
            if event == 'end':
                element.clear()
                while element.getprevious() is not None:
                    del element.getparent()[0]

    def without_sheet_object(grouped_sheets_and_rows):
        for (_, sheet_name), rows in grouped_sheets_and_rows:
            yield sheet_name, (row for (_, _, row) in rows)

    unzipped_member_files = stream_unzip(ods_chunks, chunk_size=chunk_size)
    content_xml_chunks = get_member_file(b'content.xml', unzipped_member_files)
    content_xml_file_like_obj = to_file_like_obj(content_xml_chunks)
    content_xml_parsed = etree.iterparse(content_xml_file_like_obj, events=('start', 'end'))
    sheets_and_rows = get_sheets_and_rows(content_xml_parsed)
    grouped_sheets_and_rows = groupby(sheets_and_rows, lambda row: (row[0], row[1]))

    yield from without_sheet_object(grouped_sheets_and_rows)
