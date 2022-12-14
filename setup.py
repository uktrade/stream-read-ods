import setuptools


def long_description():
    with open('README.md', 'r') as file:
        return file.read()


setuptools.setup(
    name='stream-read-ods',
    version='0.0.21',
    author='Department for International Trade',
    author_email='sre@digital.trade.gov.uk',
    description='Python function to extract data from an ODS spreadsheet on the fly - without having to store the entire file in memory or disk',
    long_description=long_description(),
    long_description_content_type='text/markdown',
    url='https://github.com/uktrade/stream-write-ods',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Topic :: System :: Archiving :: Compression',
    ],
    python_requires='>=3.7.4',
    install_requires=[
        'stream-unzip>=0.0.70',
        'lxml>=4.9.1',
    ],
    py_modules=[
        'stream_read_ods',
    ],
)
