from setuptools import setup, find_packages

setup(
    name="myproject",
    version="0.1.0",
    description="myproject",
    packages=find_packages(),
    python_requires=">=3.7",
    install_requires=[
        # see: `requirements.in` instead
    ],
    include_package_data=True,
)
