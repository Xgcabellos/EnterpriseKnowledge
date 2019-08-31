from setuptools import setup

setup(
    name='EKnowledge',
    version='0.1',
    packages=[ '' ],
    url='https://github.com/Xgcabellos/EnterpriseKnowledge.git',
    license='Copyright Xavier Garcia Cabellos & Alvaro Roman',
    author='xgarcia',
    author_email='xgcabellos@Gmail.com',
    description='Email interpreter as internal relationship of the company ', install_requires=['psutil', 'inflect',
                                                                                                'nltk', 'neo4j',
                                                                                                'vaderSentiment']
)
