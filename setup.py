from setuptools import setup

setup(
    name='mediasearch',
    version='0.0.0',
    packages=['mediasearch'],
    package_dir={'': 'src'},
    test_suite='mediasearch',
    url='',
    author='Alexey Karnachev',
    author_email='alekseykarnachev@gmail.com',
    zip_safe=True,
    install_requires=['vk', 'PyYAML', 'elasticsearch']
)

if __name__ == '__main__':
    pass
