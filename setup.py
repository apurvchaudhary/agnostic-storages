import pathlib
from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent

VERSION = "1.0"
PACKAGE_NAME = "agno_storage"
AUTHOR = "Apurv Chaudhary"
AUTHOR_EMAIL = "apurv.sirohi@gmail.com"
URL = "https://github.com/apurvchaudhary/agnostic-storages"

LICENSE = "MIT"
DESCRIPTION = "Cloud agnostic storage service"
LONG_DESCRIPTION = (HERE / "README.md").read_text()
LONG_DESC_TYPE = "text/markdown"

INSTALL_REQUIRES = [
      "boto3",
      "azure-storage-file-datalake",
      "django",
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
      classifiers=[
            'Framework :: Django :: 2.0',
            'Framework :: Django :: 3.0',
            'Development Status :: 3 - Alpha',
            'Intended Audience :: Developers',
            'Topic :: Software Development :: Libraries',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
            'Programming Language :: Python :: 3.9',
            'Operating System :: OS Independent'
      ],
)
