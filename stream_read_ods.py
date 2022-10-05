from itertools import groupby

from lxml import etree
from stream_unzip import stream_unzip


def stream_read_ods(ods_chunks, chunk_size=65536):

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

    for name, size, chunks in stream_unzip(ods_chunks):
        if name != b'content.xml':
            for chunk in chunks:
                pass
            continue

        doc = etree.iterparse(to_file_like_obj(chunks), events=('start', 'end'))

        sheet = None
        sheet_name = None

        pref = '{urn:oasis:names:tc:opendocument:xmlns:table:1.0}'
        def rows():
            for event, element in doc:
                # Starting a table
                if event == 'start' and f'{pref}table' == element.tag:
                    sheet = object()
                    sheet_name = element.attrib[f'{pref}name']

                # Starting a row
                if event == 'start' and f'{pref}table-row' == element.tag:
                    row = []

                # Ending a row
                if event == 'end' and f'{pref}table-row' == element.tag:
                    yield sheet, sheet_name, tuple(row)

                if event == 'start' and f'{pref}table-cell' == element.tag:
                    row.append('')

        grouped = groupby(rows(), lambda row: (row[0], row[1]))
        for (_, sheet_name), rows in grouped:
            yield sheet_name, (row for (_, _, row) in rows)
