# ObSave

[![Build Status](https://github.com/yourusername/obsave/workflows/CI/badge.svg)](https://github.com/yourusername/obsave/actions)
[![Coverage Status](https://coveralls.io/repos/github/yourusername/obsave/badge.svg?branch=main)](https://coveralls.io/github/yourusername/obsave?branch=main)
[![PyPI version](https://badge.fury.io/py/obsave.svg)](https://badge.fury.io/py/obsave)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/pypi/pyversions/obsave.svg)](https://pypi.org/project/obsave/)

English | [简体中文](README_zh.md)

A high-performance object storage service built with FastAPI, designed for reliability, scalability, and ease of use.

## Features

- **High Performance** - Asynchronous processing with memory caching
- **Object Storage** - Store and retrieve objects of any type
- **Smart Querying** - Complex queries via JSONPath
- **Monitoring** - Comprehensive system monitoring and metrics
- **Security** - Built-in access control and encryption
- **Real-time Monitoring** - System status monitoring and alerts

## Installation

```bash
pip install obsave
```

For development installation:

```bash
git clone https://github.com/yourusername/obsave.git
cd obsave
pip install -e ".[dev]"
```

## Quick Start

```python
from obsave import ObjectStorage

# Initialize storage
storage = ObjectStorage()

# Store an object
storage.store("my_key", {"data": "value"})

# Retrieve an object
obj = storage.get("my_key")
print(obj)  # {"data": "value"}
```

For more examples, see the [examples](examples/) directory.

## Documentation

Full documentation is available at [https://obsave.readthedocs.io/](https://obsave.readthedocs.io/).

## Development

We use a number of tools to ensure code quality:

```bash
# Install development dependencies
make install

# Run tests
make test

# Format code
make format

# Run linters
make lint

# Build documentation
make docs
```

## Project Structure

```
obsave/
├── src/
│   └── obsave/
│       ├── api/        # FastAPI routes and endpoints
│       ├── core/       # Core functionality
│       ├── storage/    # Storage backends
│       ├── monitoring/ # Monitoring and metrics
│       └── utils/      # Utility functions
├── tests/             # Test suite
├── docs/              # Documentation
├── examples/          # Example code
└── scripts/           # Utility scripts
```

## Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

For commercial use, please contact: sblig3@gmail.com

## Acknowledgments

Special thanks to all our [contributors](https://github.com/yourusername/obsave/graphs/contributors) who have helped make ObSave better.
