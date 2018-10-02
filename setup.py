from setuptools import setup

from basilisk import APP_NAME, APP_VERSION

setup(
    name=APP_NAME,
    version=APP_VERSION,
    description="Test application.",
    author="Minoru Osuka",
    author_email="minoru.osuka@gmail.com",
    license="AL2",
    packages=[
        "basilisk"
    ],
    install_requires=[
        "pysyncobj==0.3.4",
        "flask==1.0.2",
        "prometheus_client==0.3.1",
        "pickledb==0.7.2"
    ],
    test_suite="tests"
)
