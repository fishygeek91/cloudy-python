from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="cloudy-salesforce",
    version="0.1.0",
    author="Fishy Geek",
    author_email="fishygeek91@gmail.com",
    description="A Python client for Salesforce with CRUD operations",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/fishygeek91/cloudy-salesforce",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=requirements,
    include_package_data=True,
)
