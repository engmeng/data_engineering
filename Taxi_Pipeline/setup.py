from setuptools import find_packages, setup

setup(
    name="Taxi_Pipeline",
    packages=find_packages(exclude=["Taxi_Pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
