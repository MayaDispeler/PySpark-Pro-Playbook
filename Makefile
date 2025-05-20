.PHONY: setup generate-data run-examples test clean install

setup:
	python src/setup_pyspark.py

generate-data:
	python src/data_generator.py --output=./sample_data

run-examples:
	python src/pyspark_analysis_examples.py

test:
	pytest -v tests/

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

install:
	pip install -e .

all: setup generate-data run-examples 