all: dist

dist:
	python3 setup.py sdist bdist_wheel

upload: dist
	python3 -m twine upload -r pypi dist/*

test:
	python3 -m pytest -v tests/
