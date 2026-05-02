# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | ✓         |

## Reporting a Vulnerability

Do **not** open a public GitHub issue for security vulnerabilities.

Email `security@dataguard-agent.io` with:
- Description of the vulnerability
- Steps to reproduce
- Affected versions
- Impact assessment if known

You will receive an acknowledgment within 48 hours. We aim to release a patch within 14 days for critical issues.

## Scope

In scope:
- Remote code execution or privilege escalation via MCP tool calls
- Credential leakage via logs, error responses, or API responses
- `execute_remediation` authorization bypass
- SQL injection in custom SQL detector inputs
- Supply chain issues in direct dependencies

Out of scope:
- Vulnerabilities in the demo Airflow stack (it is intentionally misconfigured for demo purposes)
- Issues requiring physical access to the host
- Social engineering

## Deployment Hardening

See [ARCHITECTURE.md](ARCHITECTURE.md#security-model) for the security model.

Key production checklist:
- Run as non-root (UID 65532 in the provided Docker image)
- Mount credentials as Kubernetes secrets, not environment variables in pod specs
- Restrict `execute_remediation` to pipelines that explicitly opt in
- Enable audit logging (`AUDIT_LOG_ENABLED=true`)
- Do not expose the metrics endpoint (`:9090`) publicly
