from setuptools import setup

version = '0.0.1'

setup(name="nosqlite",
      version=version,
      description='A wrapper for sqlite3 to have schemaless, document-store features',
      classifiers=['Development Status :: 3 - Alpha',
                   'License :: OSI Approved :: MIT License',
                   'Operating System :: OS Independent',
                   'Programming Language :: Python',
                   'Topic :: Software Development :: Libraries :: Python Modules'],
      keywords='nosql sqlite nosqlite',
      author='Shaun Duncan',
      author_email='shaun.duncan@gmail.com',
      url='https://github.com/shaunduncan/nosqlite',
      license='MIT',
      py_modules=['nosqlite'],
      include_package_data=True,
)
