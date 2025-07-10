#!/usr/bin/env python3
"""
QETL SDK Setup Configuration
"""

from setuptools import setup, find_packages
import os

# Read the README file for long description
def read_readme():
    here = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
        return f.read()

# Read requirements from requirements.txt
def read_requirements():
    here = os.path.abspath(os.path.dirname(__file__))
    requirements_path = os.path.join(here, 'requirements.txt')
    if not os.path.exists(requirements_path):
        return []
    
    with open(requirements_path, encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name="qetl-sdk",
    version="0.1.0",
    author="ICON Labs",
    author_email="contact@iconlabs.ai",
    description="Python SDK for QETL (Quantum Enhanced Transform Layer) pipeline processing",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/iconlabs/qetl-sdk",
    project_urls={
        "Bug Tracker": "https://github.com/iconlabs/qetl-sdk/issues",
        "Documentation": "https://docs.qetl.ai/sdk",
        "Source Code": "https://github.com/iconlabs/qetl-sdk",
    },
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research", 
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Chemistry",
        "Topic :: Scientific/Engineering :: Physics",
    ],
    python_requires=">=3.8",
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=7.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.0",
            "black>=23.0",
            "isort>=5.12",
            "flake8>=6.0",
            "mypy>=1.0",
            "pre-commit>=3.0",
        ],
        "jupyter": [
            "jupyter>=1.0.0",
            "ipywidgets>=8.0.0",
            "matplotlib>=3.5.0",
            "seaborn>=0.11.0",
        ],
        "visualization": [
            "plotly>=5.0.0",
            "dash>=2.0.0",
            "bokeh>=3.0.0",
        ],
        "cloud": [
            "boto3>=1.26.0",
            "azure-storage-blob>=12.0.0",
            "google-cloud-storage>=2.0.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "qetl=qetl_sdk.cli:main",
        ],
    },
    include_package_data=True,
    package_data={
        "qetl_sdk": [
            "templates/*.yaml",
            "schemas/*.json",
            "examples/*.py",
        ],
    },
    keywords=[
        "quantum",
        "quantum-computing", 
        "bioinformatics",
        "drug-discovery",
        "pipeline",
        "etl",
        "data-processing",
        "machine-learning",
        "scientific-computing"
    ],
    zip_safe=False,
)
