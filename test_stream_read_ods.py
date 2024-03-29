from datetime import date, datetime
from decimal import Decimal
import pytest
from stream_write_ods import stream_write_ods

from stream_read_ods import (
    Currency,
    Percentage,
    Time,
    UnfinishedIterationError,
    UnzipError,
    MissingMIMETypeError,
    IncorrectMIMETypeError,
    MissingContentXMLError,
    InvalidContentXMLError,
    TooManyColumnsError,
    TooManySplitCells,
    StringTooLongError,
    stream_read_ods,
    simple_table,
)
from stream_zip import ZIP_32, NO_COMPRESSION_32, stream_zip


def test_stream_write_ods():
    def get_sheets():
        def get_rows_of_sheet_1():
            yield 'Value A & B', None, True, False
            yield 1, 1.2, date(2021, 1, 2), datetime(2021, 1, 2, 3, 4, 5, 6)

        yield 'Sheet 1 name', ('col_1_name', 'col_2_name', 'col_5_name', 'col_4_name'), get_rows_of_sheet_1()

        def get_rows_of_sheet_2():
            yield 'col_1_value',
            yield None,

        yield 'Sheet 2 name', ('col_1_name',), get_rows_of_sheet_2()

    ods_chunks = stream_write_ods(get_sheets())

    files = [
        (name, list(rows))
        for name, rows in stream_read_ods(ods_chunks)
    ]
    assert files == [
        ('Sheet 1 name', [
            ('col_1_name', 'col_2_name', 'col_5_name', 'col_4_name'),
            ('Value A & B', None, True, False),
            (Decimal('1'), Decimal('1.2'), date(2021, 1, 2), datetime(2021, 1, 2, 3, 4, 5, 6))
        ]),
        ('Sheet 2 name', [('col_1_name',), ('col_1_value',), ()]),
    ]

    ods_chunks = stream_write_ods(get_sheets())

    def tables():
        for name, rows in stream_read_ods(ods_chunks):
            table_columns, table_rows = simple_table(rows, skip_rows=0)
            yield name, table_columns, table_rows

    tables = [
        (name, columns, list(rows))
        for name, columns, rows in tables()
    ]
    assert tables == [
        (
            'Sheet 1 name',
            ('col_1_name', 'col_2_name', 'col_5_name', 'col_4_name'),
            [
                ('Value A & B', None, True, False),
                (Decimal('1'), Decimal('1.2'), date(2021, 1, 2), datetime(2021, 1, 2, 3, 4, 5, 6))
            ],
        ),
        (
            'Sheet 2 name',
            ('col_1_name',),
            [
                ('col_1_value',)
            ]
        ),
    ]

    ods_chunks = stream_write_ods(get_sheets())
    sheets = stream_read_ods(ods_chunks)
    sheets_it = iter(sheets)
    sheet, rows = next(sheets_it)
    with pytest.raises(UnfinishedIterationError):
        next(sheets_it)


def test_excel_export():
    def get_ods_chunks():
        with open('fixtures/excel.ods', 'rb') as f:
            while True:
                chunk = f.read(10)
                if not chunk:
                    break
                yield chunk

    files = [
        (name, list(rows))
        for name, rows in stream_read_ods(get_ods_chunks())
    ]
    assert files == [
        ('First', [
            ('Integer', 'float', 'Date', 'Datetime', False, True, 'Percentage', 'String', 'Money', 'Time', 'Empty'),
            (Decimal('1'), Decimal('4.5599999999999996'), datetime(2002, 1, 1), datetime(2001, 1, 1, 23, 23), False, True, Percentage('0.5'), '🍰', Currency('1.23'), Time(sign='+', years=0, months=0, days=0, hours=1, minutes=23, seconds=Decimal('0'))),
            (),
        ]),
        ('Second', [
            (),
        ]),
    ]

    def tables():
        for name, rows in stream_read_ods(get_ods_chunks()):
            table_columns, table_rows = simple_table(rows, skip_rows=0)
            yield name, table_columns, table_rows

    tables = [
        (name, columns, list(rows))
        for name, columns, rows in tables()
    ]
    assert tables == [
        (
            'First',
            ('Integer', 'float', 'Date', 'Datetime', False, True, 'Percentage', 'String', 'Money', 'Time', 'Empty'),
            [
                (Decimal('1'), Decimal('4.5599999999999996'), datetime(2002, 1, 1, 0, 0), datetime(2001, 1, 1, 23, 23), False, True, Decimal('0.5'), '🍰', Currency('1.23'), Time(sign='+', years=0, months=0, days=0, hours=1, minutes=23, seconds=Decimal('0')), None)
            ]
        ),
        (
            'Second',
            (),
            [],
        ),
    ]


def test_excel_with_styles_export():
    def get_ods_chunks():
        with open('fixtures/excel-with-styles.ods', 'rb') as f:
            while True:
                chunk = f.read(10)
                if not chunk:
                    break
                yield chunk

    files = [
        (name, list(rows))
        for name, rows in stream_read_ods(get_ods_chunks())
    ]
    assert files == [('Sheet1', [('Fist line\nSecondline\n\nFinal line initalic',), ()])]


def test_libreoffice_export():
    def get_ods_chunks():
        with open('fixtures/libreoffice.ods', 'rb') as f:
            while True:
                chunk = f.read(10)
                if not chunk:
                    break
                yield chunk

    files = [
        (name, list(rows))
        for name, rows in stream_read_ods(get_ods_chunks())
    ]
    assert files == [
        ('Sheet1', [
            ('integer', 'float', 'date', 'datetime', 'bool false', 'bool true', 'percentage', 'money', 'time', 'string', 'empty'),
            (Decimal('1'), Decimal('4.56'), date(2012, 1, 1), datetime(2012, 1, 1, 1, 12, 23), False, True, Percentage('0.5'), Currency('2.34', code='GBP'), Time(sign='+', years=0, months=0, days=0, hours=1, minutes=23, seconds=Decimal('0')), '🍰'),
        ]),
    ]

    def tables():
        for name, rows in stream_read_ods(get_ods_chunks()):
            table_columns, table_rows = simple_table(rows, skip_rows=0)
            yield name, table_columns, table_rows

    tables = [
        (name, columns, list(rows))
        for name, columns, rows in tables()
    ]
    assert tables == [
        (
            'Sheet1',
            ('integer', 'float', 'date', 'datetime', 'bool false', 'bool true', 'percentage', 'money', 'time', 'string', 'empty'),
            [
                (Decimal('1'), Decimal('4.56'), date(2012, 1, 1), datetime(2012, 1, 1, 1, 12, 23), False, True, Percentage('0.5'), Currency('2.34', code='GBP'), Time(sign='+', years=0, months=0, days=0, hours=1, minutes=23, seconds=Decimal('0')), '🍰', None),
            ]
        ),
    ]


def test_libreoffice_with_styles_export():
    def get_ods_chunks():
        with open('fixtures/libreoffice-with-styles.ods', 'rb') as f:
            while True:
                chunk = f.read(10)
                if not chunk:
                    break
                yield chunk

    files = [
        (name, list(rows))
        for name, rows in stream_read_ods(get_ods_chunks())
    ]
    assert files == [('Sheet1', [('First line\nSecond bold\n\nItalic\nAnother font', 'More')])]


def test_libreoffice_with_repeated_export():
    def get_ods_chunks():
        with open('fixtures/libreoffice-with-repeated.ods', 'rb') as f:
            while True:
                chunk = f.read(10)
                if not chunk:
                    break
                yield chunk

    files = [
        (name, list(rows))
        for name, rows in stream_read_ods(get_ods_chunks())
    ]
    assert files == [('Sheet1', [('to be repeated', 'to be repeated', 'to be repeated', 'to be repeated', 'after repeated')])]


def test_libreoffice_with_spanned_export():
    def get_ods_chunks():
        with open('fixtures/libreoffice-with-spanned.ods', 'rb') as f:
            while True:
                chunk = f.read(10)
                if not chunk:
                    break
                yield chunk

    files = [
        (name, list(rows))
        for name, rows in stream_read_ods(get_ods_chunks())
    ]
    assert files == [('Sheet1', [('to be spanned', 'to be spanned', 'to be spanned', 'to be spanned', 'after spanned')])]


def test_libreoffice_with_row_spanned_export():
    def get_ods_chunks():
        with open('fixtures/libreoffice-with-row-spanned.ods', 'rb') as f:
            while True:
                chunk = f.read(10)
                if not chunk:
                    break
                yield chunk

    files = [
        (name, list(rows))
        for name, rows in stream_read_ods(get_ods_chunks())
    ]
    assert files == [('Sheet1', [('Column span', 'B1'), ('Column span', 'B2'), ('After column span',)])]


def test_libreoffice_with_row_cols_spanned_repeated_export():
    def get_ods_chunks():
        with open('fixtures/libreoffice-with-row-col-spans-repeated.ods', 'rb') as f:
            while True:
                chunk = f.read(10)
                if not chunk:
                    break
                yield chunk

    files = [
        (name, list(rows))
        for name, rows in stream_read_ods(get_ods_chunks())
    ]
    assert files == [('Sheet1', [
        ('Value', 'Value', 'Value', 'Value', 'Value', 'Value', 'After G1'),
        ('Value', 'Value', 'Value', 'Value', 'Value', 'Value', 'After G2'),
        ('After A3',)])]


def test_libreoffice_with_many_merged_cells():
    def get_ods_chunks():
        with open('fixtures/libreoffice-with-many-merged-cells.ods', 'rb') as f:
            while True:
                chunk = f.read(10)
                if not chunk:
                    break
                yield chunk

    sheet, rows = next(stream_read_ods(get_ods_chunks()))
    with pytest.raises(TooManySplitCells):
       next(rows)


def test_libreoffice_doc():
    def get_chunks():
        with open('fixtures/doc.odt', 'rb') as f:
            while True:
                chunk = f.read(10)
                if not chunk:
                    break
                yield chunk

    with pytest.raises(IncorrectMIMETypeError):
        next(stream_read_ods(get_chunks()))


def test_zip_without_mimetype():

    def unzipped_files():
        modified_at = datetime.now()
        perms = 0o600

        def file_1_data():
            yield b'Some bytes 1'

        yield 'my-file-1.txt', modified_at, perms, NO_COMPRESSION_32, file_1_data()


    with pytest.raises(MissingMIMETypeError):
        next(stream_read_ods(stream_zip(unzipped_files())))


def test_zip_incorrect_mimetype_error():

    def unzipped_files():
        modified_at = datetime.now()
        perms = 0o600

        def file_1_data():
            yield b'application/vnd.oasis.opendocument.spreadsheetXX'

        yield 'mimetype', modified_at, perms, NO_COMPRESSION_32, file_1_data()

    with pytest.raises(IncorrectMIMETypeError):
        next(stream_read_ods(stream_zip(unzipped_files())))


def test_zip_no_context_xml():

    def unzipped_files():
        modified_at = datetime.now()
        perms = 0o600

        def file_1_data():
            yield b'application/vnd.oasis.opendocument.spreadsheet'

        yield 'mimetype', modified_at, perms, NO_COMPRESSION_32, file_1_data()


    with pytest.raises(MissingContentXMLError):
        next(stream_read_ods(stream_zip(unzipped_files())))


def test_zip_bad_xml():

    def unzipped_files():
        modified_at = datetime.now()
        perms = 0o600

        def file_1_data():
            yield b'application/vnd.oasis.opendocument.spreadsheet'

        yield 'mimetype', modified_at, perms, NO_COMPRESSION_32, file_1_data()

        def file_2_data():
            yield b'Not XML'

        yield 'content.xml', modified_at, perms, ZIP_32, file_2_data()


    with pytest.raises(InvalidContentXMLError):
        next(stream_read_ods(stream_zip(unzipped_files())))


def test_not_zip():

    def junk():
        yield b'Not a zip' * 1000

    with pytest.raises(UnzipError):
        next(stream_read_ods(junk()))


def test_string_limit():
    def get_sheets():
        yield 'Sheet 1 name', ('col_1_name',), (('Value A' * 100000,),)

    cols, rows = next(stream_read_ods(stream_write_ods(get_sheets())))
    next(rows)
    with pytest.raises(StringTooLongError):
        next(rows)


def test_string_limit_huge():
    def get_sheets():
        yield 'Sheet 1 name', ('col_1_name',), (('Value A' * 10000000,),)

    cols, rows = next(stream_read_ods(stream_write_ods(get_sheets())))
    next(rows)
    with pytest.raises(InvalidContentXMLError):
        next(rows)


def test_too_many_columns():
    def get_sheets():
        yield 'Sheet 1 name', ('col_1_name',) * 100000, (('Value A',),)

    cols, rows = next(stream_read_ods(stream_write_ods(get_sheets())))
    with pytest.raises(TooManyColumnsError):
        next(rows)


def test_high_depth():

    def unzipped_files():
        modified_at = datetime.now()
        perms = 0o600

        def file_1_data():
            yield b'application/vnd.oasis.opendocument.spreadsheet'

        yield 'mimetype', modified_at, perms, NO_COMPRESSION_32, file_1_data()

        def file_2_data():
            yield b'<t>' * 1000 + b'</t>' * 1000

        yield 'content.xml', modified_at, perms, ZIP_32, file_2_data()


    with pytest.raises(InvalidContentXMLError):
        next(stream_read_ods(stream_zip(unzipped_files())))
