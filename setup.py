from setuptools import setup, find_packages

setup(
    name='riberry',
    version='0.5.0',
    author='Shady Rafehi',
    url='https://github.com/srafehi/riberry',
    author_email='shadyrafehi@gmail.com',
    packages=find_packages(),
    install_requires=[
        'redis>=3.2.0,<4.0.0',
        'croniter>=0.3.28,<1.0.0',
        'pendulum>=2.0.0,<3.0.0',
        'sqlalchemy>=1.3,<1.4',
        'celery>=4.3.0,<5.0.0',
        'pyyaml',
        'pyjwt',
        'toml',
        'click',
    ],
)
