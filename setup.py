from setuptools import setup

setup(name='gl2api',
      version='0.6',
      description='A basic graylog 2 client API that is incomplete, but avoids the swagger codegen template code',
      url='https://github.com/campenberger/gl2api',
      author='Chris Ampenberger',
      author_email='campenberger@lexington-solutions.com',
      license='MIT',
      packages=['gl2api'],
      zip_safe=False,
      install_requires=['requests>=2.18', 'marshmallow==2.15.3'],
      setup_requires=['requests>=2.18', 'marshmallow==2.15.3'])
