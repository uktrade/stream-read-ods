# stream-read-ods

[![PyPI package](https://img.shields.io/pypi/v/stream-read-ods?label=PyPI%20package&color=%234c1)](https://pypi.org/project/stream-read-ods/) [![Test suite](https://img.shields.io/github/actions/workflow/status/uktrade/stream-read-ods/test.yml?label=Test%20suite)](https://github.com/uktrade/stream-read-ods/actions/workflows/test.yml) [![Code coverage](https://img.shields.io/codecov/c/github/uktrade/stream-read-ods?label=Code%20coverage)](https://app.codecov.io/gh/uktrade/stream-read-ods)

Python function to extract data from an ODS spreadsheet on the fly - without having to store the entire file in memory or disk

To construct ODS spreadsheets on the fly, try [stream-write-ods](https://github.com/uktrade/stream-write-ods).


## Installation

```bash
pip install stream-read-ods
```


## Usage

To extract the rows you must use the `stream_read_ods` function, passing it an iterable of `bytes` instances, and it will return an iterable of `(sheet_name, sheet_rows)` pairs.

```python
from stream_read_ods import stream_read_ods
import httpx

def ods_chunks():
    # Iterable that yields the bytes of an ODS file
    with httpx.stream('GET', 'https://www.example.com/my.ods') as r:
        yield from r.iter_bytes(chunk_size=65536)

for sheet_name, sheet_rows in stream_read_ods(ods_chunks()):
    for sheet_row in sheet_rows:
        print(sheet_row)  # Tuple of cells
```

If the spreadsheet is of a fairly simple structure, then the `sheet_rows` from above can be passed to the `simple_table` function to extract the names of the columns and the rows of the table.

```python
from stream_read_ods import stream_read_ods, simple_table

for sheet_name, sheet_rows in stream_read_ods(ods_chunks()):
    columns, rows = simple_table(sheet_rows, skip_rows=2)
    for row in rows:
        print(row)  # Tuple of cells
```

This can then be used to construct a Pandas dataframe from the ODS file (although this would store the entire sheet in memory).

```python
import pandas as pd
from stream_read_ods import stream_read_ods, simple_table

for sheet_name, sheet_rows in stream_read_ods(ods_chunks()):
    columns, rows = simple_table(sheet_rows, skip_rows=2)
    df = pd.DataFrame(rows, columns=columns)
    print(df)
```


## Types

There are [8 possible data types in an Open Document Spreadsheet](https://docs.oasis-open.org/office/v1.2/os/OpenDocument-v1.2-os-part1.html#attribute-office_value-type): boolean, currency, date, float, percentage, string, time, and void. These are converted to Python types according to the following table.

| ODS type    | Python type                                              |
|:------------|:---------------------------------------------------------|
| boolean     | bool                                                     |
| currency    | [stream_read_ods.Currency](#stream_read_odscurrency)     |
| date        | date or datetime                                         |
| float       | Decimal                                                  |
| percentage  | [stream_read_ods.Percentage](#stream_read_odspercentage) |
| string      | str                                                      |
| time        | [stream_read_ods.Time](#stream_read_odstime)             |
| void        | NoneType                                                 |

Note that a string in an ODS file can be structured and styled - under the hood this is with an HTML-like syntax. However, these structures and styles are not preserved by the conversion process. The exception is that each paragraph - `p` tag - after the first is converted into a newline.

### stream_read_ods.Currency

A subclass of Decimal with an additional attribute `code` that contains the currency code, for example the string `GBP`. This can be `None` if the ODS file does not specify a code.

### stream_read_ods.Percentage

A subclass of Decimal.

### stream_read_ods.Time

The Python built-in timedelta type is not used since timedelta does not offer a way to store intervals of years or months, other than converting to days which would be a loss of information.

Instead, a [namedtuple](https://docs.python.org/3/library/collections.html#collections.namedtuple) is defined, stream_read_ods.Time, with members:

| Member  | Type    |
|:--------|:--------|
| sign    | str     |
| years   | int     |
| months  | int     |
| days    | int     |
| hours   | int     |
| minutes | int     |
| seconds | Decimal |


## Merged cells

Merged cells in the spreadsheet are split, with the same value copied into all of the resulting cells. This is probably The Right Thing when converting a spreadsheet into a dataframe-like structure since such cells are usually header-like.


## Running tests

```
pip install -e ".[dev]"
pytest
```

## Exceptions

Exceptions raised by the source iterable are passed through `stream_read_ods` unchanged. Other exceptions are in the `stream_read_ods` module, and derive from its `StreamReadODSError`.


## Exception hierarchy

  - **StreamReadODSError**

    Base class for all explicitly-thrown exceptions

    - **InvalidOperationError**

      - **UnfinishedIterationError**

        The rows iterator of a sheet has not been iterated to completion

    - **InvalidODSFileError** (also inherits from the **ValueError** built-in)

      Base class for errors relating to the bytes of the ODS file not being parsable. Several errors relate to the fact that ODS files are ZIP archives that require specific members and contents.

      - **UnzipError**

        The ODS file does not appear to be a valid ZIP file. More detail is in the `__cause__` member of the raised exception, which is an exception that derives from [`UnzipValueError` in stream-unzip](https://github.com/uktrade/stream-unzip#exception-hierarchy).

      - **MissingMIMETypeError**

        The MIME type of the file was not present. In ZIP terms, this means that the first file of the ZIP archive is not named `mimetype`.

      - **IncorrectMIMETypeError**

        The MIME type was present, but does not match `application/vnd.oasis.opendocument.spreadsheet`. The can happen if a file such as an Open Document Text (ODT) file is passed rather than an ODS file.

      - **MissingContentXMLError**

        The file claims to be an ODS file according to its MIME type, but does not contain the requires `content.xml` file that contains the sheet data.

      - **InvalidContentXMLError**

        The file claims to be an ODS file according to its MIME type, it contains a `content.xml` file, but it doesn't appear to contain valid XML. More detail is in the `__cause__` member of the raised exception, which is an exception that derives from [`lxml.etree.LxmlError`](https://lxml.de/api/lxml.etree.LxmlError-class.html)

        This exception may be raised in cases the underlying XML requires a high amount of memory to be parsed.

      - **InvalidODSXMLError**

        The file has valid content as XML, but there is some aspect of the XML that makes it not parseable as a spreadsheet.

        - **InvalidTypeError**

          The data type of a cell is not one of the 8 ODS data types

        - **InvalidValueError**

          The value of a cell cannot be parsed as its declared type. More detail may be in the `__cause__` member of the raised exception.

          - **InvalidBooleanValueError**

          - **InvalidCurrencyValueError**

          - **InvalidDateValueError**

          - **InvalidFloatValueError**

          - **InvalidPercentageValueError**

          - **InvalidTimeValueError**

    - **SizeError**

      The file appears valid as an ODS file so far, but processing has hit a size related limit. These limits are in place to avoid unexpected high memory use.

      - **TooManyColumnsError**

        More columns than the `max_columns` argument to `stream_read_ods` have been encountered. The default limit is 65536.

      - **TooManySplitCells**

        When splitting merged cells, more split cells need to be created than the `max_split_cells` argument to `stream_read_ods` allows. The default limit is 65536.

      - **StringTooLongError**

        A cell with a string value that's longer than the `max_string_length` argument to `stream_read_ods` has been encountered. The default limit is 65536.
