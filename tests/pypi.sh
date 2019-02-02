# pip install setuptools wheel twine
python setup.py sdist bdist_wheel
twine upload dist/*