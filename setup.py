import os
from setuptools import setup
from setuptools import find_packages

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

queries = [os.path.join("sql", "*.sql")]
notebooks = [os.path.join("notebooks", "*.md")]

extract_app = "nifd_casfri_preprocessing.scripts.extract_casfri_data_app:main"
summary_app = "nifd_casfri_preprocessing.scripts.nifd_casfri_summary_app:main"
process_app = "nifd_casfri_preprocessing.scripts.process_for_cbm_app:main"
console_scripts = [
    "nifd_casfri_extract = " + extract_app,
    "nifd_casfri_summary = " + summary_app,
    "nifd_casfri_process = " + process_app
]

setup(
    name="nifd_casfri_preprocessing",
    version="0.8.2",
    description="nifd casfri preprocessing scripts",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=["test*"]),
    package_data={"nifd_casfri_preprocessing": queries + notebooks},
    entry_points={"console_scripts": console_scripts},
    install_requires=requirements,
)
