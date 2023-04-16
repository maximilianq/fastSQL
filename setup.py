from setuptools import setup

setup(
    name="fastsql",
    version='0.0.0.3',
    description="Library for SQL integration using annotations.",
    author="Maximilian Quaeck",
    author_email="maximilian.quaeck@gmx.de",
    keywords="sql",
    packages=['fastsql'],
    package_dir={'fastsql': 'src'},
    install_requires=[
        "psycopg-binary",
        "psycopg"
    ]
)
