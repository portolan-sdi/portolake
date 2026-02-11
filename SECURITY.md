# Security Policy

## Supported Versions

We release patches for security vulnerabilities for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |
| < 0.1   | :x:                |

## Reporting a Vulnerability

We take the security of Portolake seriously. If you believe you have found a security vulnerability, please report it to us as described below.

### Please do NOT:

- Open a public GitHub issue for security vulnerabilities
- Disclose the vulnerability publicly before it has been addressed

### Please DO:

1. **Report via GitHub Security Advisories** (preferred):
   - Go to the [Security tab](https://github.com/portolan-sdi/portolake/security/advisories)
   - Click "Report a vulnerability"
   - Fill in the details

2. **Email**: If you prefer, you can email the maintainers at:
   - nlebovits@pm.me
   - Use subject line: "SECURITY: Portolake - [brief description]"

### What to include:

- Type of vulnerability (e.g., path traversal, command injection, etc.)
- Full paths of source file(s) related to the vulnerability
- Location of the affected source code (tag/branch/commit or direct URL)
- Any special configuration required to reproduce the issue
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact of the issue, including how an attacker might exploit it

### What to expect:

- **Initial Response**: Within 48 hours
- **Status Update**: Within 7 days with our evaluation
- **Fix Timeline**: Critical issues within 30 days, others within 90 days
- **Credit**: We will acknowledge your contribution in the release notes (unless you prefer to remain anonymous)

## Security Best Practices for Users

When using Portolake:

1. **Keep Dependencies Updated**
   - Regularly update to the latest version
   - Run `uv sync` or `pip install --upgrade portolake`
   - Monitor security advisories

2. **Input Validation**
   - Validate file paths before processing
   - Be cautious when processing files from untrusted sources
   - Verify output paths to prevent overwrites

3. **File Permissions**
   - Ensure proper file permissions on output directories
   - Don't run with elevated privileges unless necessary

## Disclosure Policy

- Security vulnerabilities will be disclosed after a fix is available
- We will publish a security advisory on GitHub
- Critical vulnerabilities will be highlighted in release notes
- CVE IDs will be requested for significant vulnerabilities

## Security Updates

Security updates are delivered through:
- GitHub Security Advisories
- Release notes in CHANGELOG.md
- PyPI package updates
- GitHub Releases

## Questions?

If you have questions about security that are not vulnerabilities, please:
- Open a regular GitHub issue
- Contact maintainers via email

Thank you for helping keep Portolake and its users safe!
