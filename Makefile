.PHONY: install test lint format clean build docs

install:
	pip install -e ".[dev]"

test:
	pytest tests/ --cov=obsave --cov-report=term-missing

lint:
	flake8 src/obsave tests
	mypy src/obsave tests
	black --check src/obsave tests
	isort --check-only src/obsave tests

format:
	black src/obsave tests
	isort src/obsave tests

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete

build:
	python setup.py sdist bdist_wheel

docs:
	cd docs && make html
