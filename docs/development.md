# Development Guide

This guide will help you set up your development environment and contribute to ObSave.

## Development Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/obsave.git
cd obsave
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install development dependencies:
```bash
pip install -e ".[dev]"
```

## Code Style

We use the following tools to maintain code quality:
- Black for code formatting
- isort for import sorting
- mypy for static type checking
- flake8 for linting

Run all style checks:
```bash
make lint
```

## Testing

We use pytest for testing. Run the test suite:
```bash
make test
```

For coverage report:
```bash
make coverage
```

## Documentation

We use Sphinx for documentation. To build the docs:
```bash
cd docs
make html
```

## Git Workflow

1. Create a new branch for your feature:
```bash
git checkout -b feature/your-feature-name
```

2. Make your changes and commit them:
```bash
git add .
git commit -m "feat: your descriptive commit message"
```

We follow [Conventional Commits](https://www.conventionalcommits.org/) for commit messages.

3. Push your changes and create a pull request:
```bash
git push origin feature/your-feature-name
```

## Release Process

1. Update CHANGELOG.md
2. Create a new git tag
3. Push to GitHub
4. GitHub Actions will automatically build and publish to PyPI

## Need Help?

- Open an issue for bugs or feature requests
- Join our community discussions
- Read our [Contributing Guidelines](../CONTRIBUTING.md)
