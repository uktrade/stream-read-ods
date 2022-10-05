from stream_unzip import stream_unzip

def stream_read_ods(ods_chunks, chunk_size=65536):
    yield from stream_unzip(ods_chunks)
