from datetime import date, datetime
from decimal import Decimal
import pytest
from stream_write_ods import stream_write_ods

from stream_read_ods import (
    Currency,
    Percentage,
    Time,
    UnzipError,
    MissingMIMETypeError,
    IncorrectMIMETypeError,
    MissingContentXMLError,
    stream_read_ods,
    simple_table,
)
from stream_zip import NO_COMPRESSION_32, stream_zip


def test_stream_write_ods():
    def get_sheets():
        def get_rows_of_sheet_1():
            yield 'Value A', None, True, False
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
            ('Value A', None, True, False),
            (Decimal('1'), Decimal('1.2'), date(2021, 1, 2), datetime(2021, 1, 2, 3, 4, 5, 6))
        ]),
        ('Sheet 2 name', [('col_1_name',), ('col_1_value',), (None,)]),
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
                ('Value A', None, True, False),
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
            ('Integer', 'float', 'Date', 'Datetime', False, True, 'Percentage', 'String', 'Money', 'Time', 'Empty', None),
            (Decimal('1'), Decimal('4.5599999999999996'), datetime(2002, 1, 1), datetime(2001, 1, 1, 23, 23), False, True, Percentage('0.5'), '🍰', Currency('1.23'), Time(sign='+', years=0, months=0, days=0, hours=1, minutes=23, seconds=Decimal('0')), None),
            (None, ),
        ]),
        ('Second', [
            (None,),
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
            (Decimal('1'), Decimal('4.56'), date(2012, 1, 1), datetime(2012, 1, 1, 1, 12, 23), False, True, Percentage('0.5'), Currency('2.34', code='GBP'), Time(sign='+', years=0, months=0, days=0, hours=1, minutes=23, seconds=Decimal('0')), '🍰', None),
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


def test_not_zip():

    def junk():
        yield b'Not a zip' * 1000

    with pytest.raises(UnzipError):
        next(stream_read_ods(junk()))
