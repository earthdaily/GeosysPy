test:
	pytest

build:
	python setup.py sdist bdist_wheel

check_build:
	twine check dist/*