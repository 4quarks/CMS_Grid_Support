# coding=utf-8

from setuptools import setup
from cms_support.utils.constants import Constants

#  long_description=open('README.md').read(),
#  https://betterscientificsoftware.github.io/python-for-hpc/tutorials/python-pypi-packaging/

setup(
    name=Constants.PACKAGE_NAME,
    version=Constants.VERSION,
    author=Constants.AUTHOR,
    author_email=Constants.EMAIL,
    packages=[Constants.PACKAGE_NAME, Constants.PACKAGE_NAME+'.sites', Constants.PACKAGE_NAME+'.transfers'],
    scripts=[],
    url=Constants.URL_PROJECT,
    license='LICENSE',
    description='Tools to accelerate monitoring in the CMS computing grid',
    install_requires=open('requirements.txt').read().split("\n"),
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
)
















