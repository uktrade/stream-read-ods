# stream-read-ods [![CircleCI](https://circleci.com/gh/uktrade/stream-read-ods.svg?style=shield)](https://circleci.com/gh/uktrade/stream-read-ods) [![Test Coverage](https://api.codeclimate.com/v1/badges/939edbf3c528fa861951/test_coverage)](https://codeclimate.com/github/uktrade/stream-read-ods/test_coverage)

Python function to extract data from an ODS spreadsheet on the fly - without having to store the entire file in memory or disk

To construct ODS spreadsheets on the fly, try [stream-write-ods](https://github.com/uktrade/stream-write-ods).


## Installation

```bash
pip install stream-read-ods
```


## Usage

```python
from stream_read_ods import stream_read_ods
import httpx

def ods_chunks():
    # Iterable that yields the bytes of an ODS file
    with httpx.stream('GET', 'https://www.example.com/my.ods') as r:
        yield from r.iter_bytes(chunk_size=65536)

for name, rows in stream_read_ods(ods_chunks()):
    print(name)  # Sheet name
    for row in rows:
        print(row)  # Tuple of cells
```


## Types

There are [8 possible data types in an Open Document Spreadsheet](https://docs.oasis-open.org/office/v1.2/os/OpenDocument-v1.2-os-part1.html#attribute-office_value-type): boolean, currency, date, float, percentage, string, time, and void. 4 of these can be output by stream-read-ods, chosen automatically according to the following table.

| ODS type    | Python type                |
|:------------|:---------------------------|
| boolean     | boolean                    |
| currency    | stream_read_ods.Currency   |
| date        | date or datetime           |
| float       | Decimal                    |
| percentage  | stream_read_ods.Percentage |
| string      | str                        |
| time        | stream_read_ods.Time       |
| void        | NoneType                   |


## Running tests

```
pip install -r requirements-dev.txt
pytest
```
