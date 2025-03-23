"""Setup file for github-api-archive package."""

from setuptools import setup, find_packages

setup(
    name="github-api-archive",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "sqlalchemy",
        "requests",
        "google-cloud-bigquery",
        "pandas",
        "numpy",
        "tqdm"
    ],
    extras_require={
        "dev": [
            "pytest",
            "pytest-asyncio",
            "black",
            "flake8",
            "mypy"
        ]
    }
)
