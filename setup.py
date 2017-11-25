from setuptools import setup, find_packages
import itertools

options = dict(
    name='trainxtract',
    version='0.0.1',
    packages=find_packages(),
    license='MIT',
    install_requires = ['pandas', 'click'],
    entry_points = {
        'console_scripts' : [
            'trainxtract = trainxtract:run_app',
            'trainxtract-help = trainxtract:run_help'
            'trainxtract-final = trainxtract:run_final'
        ]
    }
)

setup(**options)
