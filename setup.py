#!/usr/bin/env python

import os
import sys
import shutil
from setuptools import setup

import versioneer

#------------------------------------------------------------------------
# Optional building with MAVEN
#------------------------------------------------------------------------

JAVA_SRC = "knit_jvm"
jar_file = os.path.join(JAVA_SRC,"target", "knit-1.0-SNAPSHOT.jar")

with open('requirements.txt') as f:
    requires = f.read().splitlines()

if 'mvn' in sys.argv:

    # JAVA_HOME necessary for building
    if not os.environ.get("JAVA_HOME"):
        print("PLEASE SET JAVA_HOME")
        sys.exit(1)

    os.chdir(JAVA_SRC)
    build_cmd = "mvn clean install -q"
    os.system(build_cmd)
    os.chdir("..")
    sys.argv.remove("mvn")

    java_lib_dir = os.path.join("knit","java_libs")
    if not os.path.exists(java_lib_dir):
        os.mkdir(java_lib_dir)
    shutil.copy(jar_file,java_lib_dir)

setup(name='knit',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='Python wrapper for YARN Application: distributed shell',
      url='http://github.com/dask/knit/',
      maintainer='Benjamin Zaitlen',
      maintainer_email='bzaitlen@continuum.io',
      license='BSD',
      keywords='yarn',
      packages=['knit'],
      package_data={'knit': ['java_libs/knit-1.0-SNAPSHOT.jar']},
      install_requires=requires,
      long_description=(open('README.rst').read() if os.path.exists('README.rst')
                        else ''),
      zip_safe=False)
