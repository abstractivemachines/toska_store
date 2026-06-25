# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in ToskaStore, please report it through GitHub's private vulnerability reporting feature:

1. Go to the repository's **Security** tab
2. Click **Report a vulnerability**
3. Provide a detailed description of the vulnerability

We will acknowledge your report and work with you to understand and address the issue. Please do not disclose the vulnerability publicly until we have had a chance to address it.

## Supported Versions

Security updates are provided for the latest release only.

## Security Best Practices

When deploying ToskaStore:

- Use scoped read, write, admin, and replication authentication tokens for production deployments
- Store tokens outside of source control using environment variables
- Restrict network access to trusted clients only
- Use TLS/HTTPS when exposing ToskaStore over the network
