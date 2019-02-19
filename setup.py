from setuptools import setup

required=[
    'requests>=2.14.2',
    'redis>=2.10.5',
    'boto3',
    'kinesis-python>=0.1.8'
]

setup(name='quantum',
      version='1.0.0',
      description='Quantum - Fast Time Series Data Aggregation Framework',
      url='http://github.marqeta.com/marqeta/quantum',
      author='Rob Tan',
      author_email='rtan@marqeta.com',
      license='MIT',
      packages=['quantum'],
      package_data={
        '': []
      },
      zip_safe=False,
      install_requires=[
        required
      ],
      entry_points = {
        'console_scripts': ['q=quantum.main_ql:run_ql',
                            'quantum=quantum.main_quantum:run_quantum']
      }
)
