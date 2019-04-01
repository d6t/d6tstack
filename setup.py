from setuptools import setup

extras = {
    'xls': ['openpyxl','xlrd'],
    'parquet': ['pyarrow'],
    'psql': ['psycopg2-binary'],
    'mysql': ['mysql-connector'],
}

setup(
    name='d6tstack',
    version='0.1.7',
    packages=['d6tstack'],
    url='https://github.com/d6t/d6tstack',
    license='MIT',
    author='DataBolt Team',
    author_email='support@databolt.tech',
    description='d6tstack: Quickly ingest CSV and XLS files. Export to pandas, SQL, parquet',
    long_description='Quickly ingest raw files. Works for XLS, CSV, TXT which can be exported to CSV, Parquet, SQL and Pandas. d6tstack solves many performance and schema problems typically encountered when ingesting raw files.',
    install_requires=[
        'numpy','pandas>=0.22.0','sqlalchemy','scipy','d6tcollect'
    ],
    extras_require=extras,
    include_package_data=True,
    python_requires='>=3.5',
    keywords=['d6tstack', 'ingest csv'],
    classifiers=[]
)
