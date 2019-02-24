from setuptools import setup, find_packages

setup(
    name='riberry',
    version='0.4.1',
    author='Shady Rafehi',
    url='https://github.com/srafehi/riberry',
    author_email='shadyrafehi@gmail.com',
    packages=find_packages(),
    install_requires=[
        'redis>=3.0.0',
        'croniter',
        'pendulum',
        'sqlalchemy',
        'celery>=4.3.0rc1,<5.0.0',
        'pyyaml',
        'pyjwt',
        'toml',
        'click',
    ],
)
