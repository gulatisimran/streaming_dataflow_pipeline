import setuptools

with open('requirements.txt') as fp:
    REQUIRED_PACKAGES = fp.read()

setuptools.setup(
    name='ETL',
    version='1.0',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    py_modules=['cxl_pipeline']
)
