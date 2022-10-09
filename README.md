# stream-read-ods [![CircleCI](https://circleci.com/gh/uktrade/stream-read-ods.svg?style=shield)](https://circleci.com/gh/uktrade/stream-read-ods) [![Test Coverage](https://api.codeclimate.com/v1/badges/939edbf3c528fa861951/test_coverage)](https://codeclimate.com/github/uktrade/stream-read-ods/test_coverage)

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
        print(row)  # Tuple of cells
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


## Running tests

```
pip install -r requirements-dev.txt
pytest
```
