from datetime import date, datetime

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
                    parse_boolean(cell_element.attrib[f'{ns_office}boolean-value']) if value_type == 'boolean' else \
                    parse_date(cell_element.attrib[f'{ns_office}date-value']) if value_type == 'date' else \
                    parse_float(cell_element.attrib[f'{ns_office}value']) if value_type == 'float' else \
                    parse_float(cell_element.attrib[f'{ns_office}value']) if value_type == 'percentage' else \
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

        def parse_float(value):
            try:
                return int(value)
            except ValueError:
                return float(value)

        def parse_boolean(value):
            return \
                True if value == 'true' else \
                False if value == 'false' else \
                value_error(value)

        def parse_date(value):
            try:
                return date.fromisoformat(value)
            except ValueError:
                return datetime.fromisoformat(value)

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
                yield sheet_name, table_rows(parsed_xml_it)

            clear_mem(event, element)

    unzipped_member_files = stream_unzip(ods_chunks, chunk_size=chunk_size)
    content_xml_chunks = get_member_file(b'content.xml', unzipped_member_files)
    content_xml_file_like_obj = to_file_like_obj(content_xml_chunks)
    content_xml_parsed = etree.iterparse(content_xml_file_like_obj, events=('start', 'end'))
    yield from get_sheets_and_rows(content_xml_parsed)
