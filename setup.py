from setuptools import setup, find_packages

setup(
    name='riberry',
    version='0.0.1',
    author='Shady Rafehi',
    url='https://github.com/srafehi/riberry',
    author_email='shadyrafehi@gmail.com',
    packages=find_packages(),
    install_requires=[
        'redis',
        'croniter',
        'pendulum',
        'sqlalchemy',
        'celery==4.2.0rc4',
        'pyyaml',
        'pyjwt',
        'toml',
        'click',
    ],
)