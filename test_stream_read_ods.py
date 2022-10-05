from stream_write_ods import stream_write_ods
from stream_read_ods import stream_read_ods


def test_sheets():
    def get_sheets():
        def get_rows_of_sheet_1():
            yield 'Value A', 'Value B'
            yield 'Value C', 'Value D'

        yield 'Sheet 1 name', ('col_1_name', 'col_2_name'), get_rows_of_sheet_1()

        def get_rows_of_sheet_2():
            yield 'col_1_value',

        yield 'Sheet 2 name', ('col_1_name',), get_rows_of_sheet_2()

    ods_chunks = stream_write_ods(get_sheets())

    files = [
        (name, list(rows))
        for name, rows in stream_read_ods(ods_chunks)
    ]
    assert files == [
        ('Sheet 1 name', [('', ''), ('', ''), ('', '')]),
        ('Sheet 2 name', [('',), ('',)]),
    ]
