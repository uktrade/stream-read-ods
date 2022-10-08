from datetime import date, datetime
from stream_write_ods import stream_write_ods
from stream_read_ods import stream_read_ods


def test_stream_write_ods():
    def get_sheets():
        def get_rows_of_sheet_1():
            yield 'Value A', None, True, False
            yield 1, 1.2, date(2021, 1, 2), datetime(2021, 1, 2, 3, 4, 5, 6)

        yield 'Sheet 1 name', ('col_1_name', 'col_2_name', 'col_5_name', 'col_4_name'), get_rows_of_sheet_1()

        def get_rows_of_sheet_2():
            yield 'col_1_value',

        yield 'Sheet 2 name', ('col_1_name',), get_rows_of_sheet_2()

    ods_chunks = stream_write_ods(get_sheets())

    files = [
        (name, list(rows))
        for name, rows in stream_read_ods(ods_chunks)
    ]
    assert files == [
        ('Sheet 1 name', [
            ('col_1_name', 'col_2_name', 'col_5_name', 'col_4_name'),
            ('Value A', None, True, False),
            (1, 1.2, date(2021, 1, 2), datetime(2021, 1, 2, 3, 4, 5, 6))
        ]),
        ('Sheet 2 name', [('col_1_name',), ('col_1_value',)]),
    ]
