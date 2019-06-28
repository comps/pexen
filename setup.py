import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pexen",
    version="0.0.1",
    author="Jiri Jaburek",
    author_email="comps@nomail.dom",
    description="Python EXecution ENvironment, scheduler included",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/comps/pexen",
    packages=["pexen"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
