#!/usr/bin/env python

from setuptools import setup, find_packages
import os


base_dir = os.path.dirname(os.path.abspath(__file__))

setup(name='schedular',
    version='1.0.0',
    description='Scheduling module.',
    author='Jonathon Scanes',
    author_email='me@jscanes.com',
    packages=find_packages(),
    zip_safe=False,
    install_requires=[
        'pyyaml',
        'pika',
        'requests'
    ],
    package_data={
        '': ['*.yaml']
    },
    test_suite='nose.collector',
    tests_require=['nose'],
    entry_points={
        'console_scripts': [
            'scheduler = scheduler.main:main'
        ]
    }
)

