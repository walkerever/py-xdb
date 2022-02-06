import pathlib
from setuptools import setup,find_packages

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="py-dbx",
    version="0.1",
    description="generic database client for CLI lovers",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/walkerever/pydbx",
    author="Yonghang Wang",
    author_email="wyhang@gmail.com",
    license="MIT License",
    classifiers=["License :: OSI Approved :: Apache Software License"],
    packages=find_packages(),
    include_package_data=True,
    install_requires=[ "xtable","sqlalchemy","tabulate"],
    keywords=[ "database","client","sql","CLI" ],
    entry_points={ "console_scripts": 
        [ 
            "dbx=dbx:dbx_main", 
        ] 
    },
)
