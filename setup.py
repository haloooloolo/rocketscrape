# Always prefer setuptools over distutils
from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()
# Get the long description from the README file
long_description = (here / "README.md").read_text(encoding="utf-8")

setup(
    name="rocketscrape",
    version="1.0.0",
    description="Discord message stream processing framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="haloooloolo",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.11, <4",
    install_requires=[
        "discord.py-self @ git+https://github.com/dolfies/discord.py-self.git",
        "matplotlib",
        "tqdm"
    ],
    entry_points={
        'console_scripts': [
            'rocketscrape = rocketscrape:main',
        ],
    }
)
