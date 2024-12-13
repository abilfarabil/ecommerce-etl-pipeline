from setuptools import setup, find_packages

setup(
    name="ecommerce_analytics",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'pandas',
        'numpy',
        'psycopg2-binary',
        'apache-airflow',
        'python-dotenv',
        'great-expectations',
        'streamlit',
        'plotly'
    ],
)