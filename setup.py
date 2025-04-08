import pathlib
from setuptools import setup

readme = pathlib.Path(__file__).parent.resolve() / "README.md"

setup(
    name="rocketscrape",
    version="1.2.0",
    description="Discord message stream processing framework",
    long_description=readme.read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    author="haloooloolo",
    packages=["rocketscrape"],
    py_modules=[],
    python_requires=">=3.9, <4",
    install_requires=[
        "discord.py-self @ git+https://github.com/haloooloolo/discord.py-self.git",
        "matplotlib",
        "tqdm",
        "tabulate",
    ],
    entry_points={
        'console_scripts': [
            'rocketscrape = rocketscrape.__main__:main',
        ],
    }
)
