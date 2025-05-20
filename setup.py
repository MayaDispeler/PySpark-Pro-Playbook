from setuptools import setup, find_packages

setup(
    name="enterprise-pyspark",
    version="1.0.0",
    description="Enterprise-grade PySpark examples and utilities",
    author="Data Engineering Team",
    author_email="your.email@example.com",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyspark>=3.4.0",
        "pandas>=1.5.0",
        "numpy>=1.24.0",
        "faker>=18.0.0",
        "matplotlib>=3.7.1",
        "scikit-learn>=1.2.2",
    ],
    python_requires=">=3.8",
) 