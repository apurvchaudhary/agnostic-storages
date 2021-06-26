import pathlib
from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent

VERSION = "1.1"
PACKAGE_NAME = "agnostic_storages"
AUTHOR = "Apurv Chaudhary"
AUTHOR_EMAIL = "apurv.sirohi@gmail.com"
URL = "https://github.com/apurvchaudhary/agnostic-storages"

LICENSE = "MIT"
DESCRIPTION = "Cloud agnostic storage service"
LONG_DESCRIPTION = (HERE / "README.md").read_text()
LONG_DESC_TYPE = "text/markdown"

INSTALL_REQUIRES = [
      "boto3",
]

setup(name=PACKAGE_NAME,
      version=VERSION,
      description=DESCRIPTION,
      long_description=LONG_DESCRIPTION,
      long_description_content_type=LONG_DESC_TYPE,
      author=AUTHOR,
      license=LICENSE,
      author_email=AUTHOR_EMAIL,
      url=URL,
      install_requires=INSTALL_REQUIRES,
      packages=find_packages(exclude=['tests*']),
      )
