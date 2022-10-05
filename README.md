# stream-read-ods

Python function to extract data from an ODS spreadsheet on the fly - without having to store the entire file in memory or disk

> Work in progress. This README serves as a rough design spec


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
