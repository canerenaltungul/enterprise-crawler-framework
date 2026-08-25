# Security Policy

Enterprise Crawler Framework is infrastructure software that may interact with
remote services, local files, persistent state, plugins, and event queues.

Security reports are treated separately from ordinary bug reports.

## Supported Versions

| Version | Security Support |
| --- | --- |
| 1.0.x | Supported |
| < 1.0 | Not supported |

Only the latest patch release of a supported minor version is guaranteed to
receive security fixes.

## Reporting a Vulnerability

Do not publish exploit details, credentials, tokens, private data, or
proof-of-concept attacks in a public issue.

Preferred reporting path:

1. Use the repository's private vulnerability reporting feature from the
   GitHub **Security** tab when it is available.
2. Include the affected framework version.
3. Describe the vulnerable component and expected security boundary.
4. Include minimal reproduction steps.
5. Explain the potential impact.
6. Include a proposed mitigation if one is known.

If private vulnerability reporting is not available, open a public issue that
contains no vulnerability details and request a private contact channel from
the maintainer.

## Sensitive Information

Never include the following in a report:

- production passwords
- API keys
- session cookies
- private tokens
- customer data
- confidential documents
- private infrastructure addresses
- exploitable credentials

Use sanitized fixtures whenever possible.

## Security Boundaries

The framework intentionally follows several fail-closed principles.

Injected dependencies are not automatically owned or closed by consumers.

Plugin discovery and plugin loading are separate operations. Discovery of
entry-point metadata must not require importing third-party plugin code.

Event queue ownership is represented through claim tokens. A stale claim token
must not acknowledge or negatively acknowledge a recovered message.

Dead-letter transfer must preserve the source event when destination storage
fails.

Storage and plugin systems are opt-in.

TLS verification is enabled by default. Disabling TLS verification should be
treated as an explicit operator decision.

## Coordinated Disclosure

Please allow reasonable time for investigation, patch preparation, regression
testing, and release before publicly disclosing a confirmed vulnerability.

The maintainer may request additional reproduction information when necessary.

## Non-Security Bugs

Crashes, validation errors, incorrect documentation, performance problems, and
ordinary functional defects that do not create a security impact should be
reported through the normal issue tracker instead of the security channel.