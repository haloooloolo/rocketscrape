from setuptools import setup, find_packages
import pathlib

readme = pathlib.Path(__file__).parent.resolve() / "README.md"

setup(
    name="rocketscrape",
    version="1.1.0",
    description="Discord message stream processing framework",
    long_description=readme.read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    author="haloooloolo",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.9, <4",
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
