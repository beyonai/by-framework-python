# Contributing Guide

Thank you for your interest in **by-framework-python**! We welcome contributions from the community.

## 🛠️ Development Environment

Before you start contributing, please ensure your development environment meet the following requirements:

- **Python**: 3.12 or higher
- **uv**: Latest version (recommended for dependency management)
- **Redis**: 7.0 or higher (for local testing)
- **pre-commit**: Required for maintaining code quality
- **IDE**: VS Code or PyCharm is recommended

## 🚀 Contribution Flow

1. **Fork the repository** to your own GitHub account.
2. **Clone the repository** locally:
   ```bash
   git clone https://github.com/your-username/by-framework-python.git
   ```
3. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```
4. **Development**:
   - Install dependencies: `make install`
   - Install pre-commit hooks: `uv run pre-commit install`
   - Ensure your code follows the style rules: `make format` & `make lint`.
   - Please write unit tests for new features or bug fixes.
5. **Run Tests**:
   ```bash
   make test
   ```
6. **Submit Changes**:
   ```bash
   git commit -m "feat: describe your changes"
   ```
   *We recommend using [Conventional Commits](https://www.conventionalcommits.org/).*
7. **Push to Remote**:
   ```bash
   git push origin feature/your-feature-name
   ```
8. **Raise a Pull Request**: Go to the original repository and create a PR. Please follow the PR template.

## 📏 Code Style

We use `ruff`, `isort`, and `pyink` to enforce consistent code style.
Please run the following command before submitting a PR:
```bash
make format
make lint
```

## 🐛 Reporting Issues

If you find a bug or have a feature suggestion, please submit it via [GitHub Issues](https://github.com/beyonai/by-framework-python/issues).
Please provide detailed descriptions and reproduction steps.

## 📄 License

By contributing to this project, you agree that your contributions will be licensed under the [Apache 2.0 License](LICENSE).
