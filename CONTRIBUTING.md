# Contributing to StreamDQ

Thank you for your interest in contributing to StreamDQ! This document provides guidelines for contributing to the project.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/yourusername/streamdq.git`
3. Create a feature branch: `git checkout -b feature/your-feature-name`
4. Make your changes
5. Run tests: `mvn test`
6. Commit your changes: `git commit -am 'Add some feature'`
7. Push to the branch: `git push origin feature/your-feature-name`
8. Create a Pull Request

## Development Setup

### Prerequisites

- Java 11 or later
- Maven 3.6 or later
- Apache Flink 1.18 or later

### Building

```bash
mvn clean install
```

### Running Tests

```bash
mvn test
```

## Code Style

- Follow standard Java coding conventions
- Use meaningful variable and method names
- Add Javadoc comments for public APIs
- Keep methods focused and concise
- Write unit tests for new features

## Pull Request Process

1. Ensure all tests pass
2. Update documentation if needed
3. Add examples for new features
4. Describe your changes in the PR description
5. Link any related issues

## Feature Requests

Have an idea for a new feature? Open an issue with:

- Clear description of the feature
- Use cases and examples
- Expected behavior

## Bug Reports

Found a bug? Open an issue with:

- Steps to reproduce
- Expected behavior
- Actual behavior
- Environment details (Flink version, Java version, etc.)

## Areas for Contribution

- New data quality checks
- Additional anomaly detection algorithms
- Performance improvements
- Documentation improvements
- Example applications
- Integration with other frameworks

## Questions?

Feel free to open an issue for questions or discussions!
