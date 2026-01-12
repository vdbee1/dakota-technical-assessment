from setuptools import find_packages, setup

setup(
    name="energy_orchestration",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster-webserver",
        "dagster-dbt",
        "pandas",
        "requests",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)