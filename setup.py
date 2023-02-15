from setuptools import setup

setup(
    # Needed to silence warnings (and to be a worthwhile package)
    name='mictlanx',
    url='https://github.com/nachocode/mictlanx-client',
    author='Nacho Castillo',
    author_email='jesus.castillo.b@cinvestav.mx',
    # Needed to actually package something
    packages=[
        # "mictlanx",
        'src.logger',
        "src.dto"
    ],
    # Needed for dependencies
    install_requires=['numpy',],
    # *strongly* suggested for sharing
    version='0.0.1',
    # The license can be anything you like
    license='MIT',
    description='Client for communication with mictlanx storage system.',
    # We will also need a readme eventually (there will be a warning)
    # long_description=open('README.txt').read(),
)
