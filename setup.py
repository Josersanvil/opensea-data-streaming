# Setup.py script to install all the local modules from src/
from setuptools import find_packages, setup


def read_requirements():
    with open("requirements.txt") as req:
        content = req.read()
        requirements = content.split("\n")
    return requirements


if __name__ == "__main__":

    setup(
        name="opensea-data-streaming",
        version="1.0.0",
        packages=find_packages("src"),
        install_requires=read_requirements(),
        package_dir={"": "src"},
        entry_points={
            # Add the entry for the main script in opensea_client.cli.__main__.py
            "console_scripts": [
                "opensea-client = opensea_client.cli.main:main",
            ],
        },
    )
