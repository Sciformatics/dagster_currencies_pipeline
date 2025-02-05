from setuptools import find_packages, setup

setup(
    name="dagster_pipeline__currencies",
    packages=find_packages(exclude=["dagster_pipeline__currencies_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
