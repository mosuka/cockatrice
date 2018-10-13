# -*- coding: utf-8 -*-

# Copyright (c) 2018 Minoru Osuka
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 		http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup

import os

from cockatrice import NAME, VERSION

with open(os.path.join(os.path.abspath(os.path.dirname(__file__)),  'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name=NAME,
    version=VERSION,
    description='Full text search and indexing server.',
    long_description=long_description,
    author='Minoru Osuka',
    author_email='minoru.osuka@gmail.com',
    license='AL2',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Natural Language :: Japanese',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3 :: Only',
        'Topic :: Database',
        'Topic :: Database :: Database Engines/Servers',
        'Topic :: Internet',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Internet :: WWW/HTTP :: HTTP Servers',
        'Topic :: Internet :: WWW/HTTP :: Indexing/Search',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Software Development',
        'Topic :: System',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Clustering',
        'Topic :: Text Processing',
        'Topic :: Text Processing :: Indexing'
    ],
    url='https://github.com/mosuka/cockatrice',
    packages=[
        'cockatrice'
    ],
    platforms='any',
    install_requires=[
        'pysyncobj==0.3.4',
        'flask==1.0.2',
        'prometheus_client==0.3.1',
        'whoosh==2.7.4',
        'pyyaml==3.13',
        'janome==0.3.6'
    ],
    entry_points={
        'console_scripts': [
            'cockatrice = cockatrice.__main__:main'
        ]
    },
    test_suite='tests'
)
