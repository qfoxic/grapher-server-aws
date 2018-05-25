from setuptools import setup


setup(
    name='grapher-aws',
    version='2.0.0',
    license='Apache Software License',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: System :: Monitoring',
        'Topic :: System :: Systems Administration',
        'Programming Language :: Python :: 3.6',
    ],
    keywords=['data', 'visualization', 'monitoring', 'aws', 'graphs'],
    python_requires='~=3.6',
    description='Grapher driver which cover AWS infrastructure',
    entry_points={'grapher.drivers': 'aws = grapher.aws.driver:AWSDriver'},
    author='Volodymyr Paslavskyy',
    author_email='qfoxic@gmail.com',
    packages=['grapher.aws'],
    install_requires=['boto3==1.4.7', 'grapher-core==2.0.0'],
    url='https://github.com/qfoxic/grapher-server-aws',
    download_url='https://github.com/qfoxic/grapher-server-aws/archive/2.0.0.tar.gz'
)
