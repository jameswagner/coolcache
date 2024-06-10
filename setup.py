from setuptools import setup, find_packages

setup(
    name='coolcache',
    version='1.0',
    description='A client library for the coolcache program',
    author='Your Name',
    author_email='your@email.com',
    packages=find_packages(include=['app', 'app.*']),
    install_requires=[
        # Add any dependencies here, e.g.:
        # 'requests',
    ],
    entry_points={
        'console_scripts': [
            'coolcache-cli = app.client.client_main:main',
            'coolcache-server = app.main:main',
        ],
        
    },
    python_requires='>=3.6',  # Specify the Python version compatibility
)