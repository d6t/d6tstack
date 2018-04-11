from setuptools import setup

setup(
    name='d6tstack',
    version='0.1.0',
    packages=['d6t', 'd6t.stack', 'tests'],
    url='https://github.com/d6t/d6tstack',
    license='MIT',
    author='DataBolt Team',
    author_email='sales@databolt.tech',
    description='Databolt Python Library',
    long_description='Databolt python library - accelerate data engineering. '
                     'DataBolt provides tools to reduce the time it takes to get your data ready for '
                     'evaluation and analysis.',
    install_requires=[
        'numpy',
        'pandas',
        'openpyxl',
        'xlrd',
        'xlwt'
    ],
    include_package_data=True,
    python_requires='>=3.6'
)
