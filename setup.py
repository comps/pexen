import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pexen",
    version="0.0.3",
    author="Jiri Jaburek",
    author_email="comps@nomail.dom",
    description="Python EXecution ENvironment, scheduler included",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/comps/pexen",
    packages=setuptools.find_packages(),
    setup_requires=["pytest-runner"],
    tests_require=["pytest", "psutil"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3 :: Only",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
