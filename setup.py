from setuptools import setup, find_packages

setup(
    name="ssis-migrator",
    version="0.1.0",
    description="SSIS to Airflow/dbt Migration Tool",
    author="AI Assistant",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "lxml>=4.9.0",
        "pydantic>=2.0.0", 
        "click>=8.0.0",
        "jinja2>=3.1.0",
        "sqlparse>=0.4.0",
        "pyyaml>=6.0",
        "jsonschema>=4.0.0",
        "typing-extensions>=4.0.0",
        "anthropic>=0.7.0",
        "python-dotenv>=1.0.0",
    ],
    entry_points={
        "console_scripts": [
            "ssis-migrate=cli.main:main",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Code Generators",
        "Topic :: Database",
    ],
)