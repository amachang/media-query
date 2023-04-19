from setuptools import setup, find_packages

setup(
    name="media_scrapy",
    version="0.1.0",
    packages=find_packages(),
    install_requires=["scrapy~=2.8.0", "schema~=0.7.5", "typed-argument-parser~=1.8.0"],
    author="Hitoshi Amano",
    author_email="seijro@gmail.com",
    description="media scraping tool",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/amachang/media_scrapy",
    classifiers=[],
)
