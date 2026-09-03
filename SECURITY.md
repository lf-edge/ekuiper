# Security Policy

eKuiper is an LF Edge project. Please report security issues privately so that maintainers can investigate and ship a fix before the details are public.

## Reporting a Vulnerability

**Do not open a public GitHub issue, discussion, or pull request for a security vulnerability.**

### Preferred: GitHub private vulnerability reporting

1. Open [Report a vulnerability](https://github.com/lf-edge/ekuiper/security/advisories/new).
2. Include the affected version(s), a clear description, and steps to reproduce. Proof-of-concept details and suggested fixes are welcome.

This creates a private advisory visible to maintainers.

### Emergency reporting

If you believe a vulnerability is **actively exploited**, or you have found a **severe incident** (for example, compromise of the release process, signing keys, CI, or GitHub organization), contact the project security contact immediately **in addition to** the private GitHub report:

- Jiyong Huang (huangjy@emqx.io)
- Put `ACTIVELY EXPLOITED` or `SEVERE INCIDENT` in the subject line and in the GitHub report title.

Do not wait for a fix before sending that notice. Maintainers will coordinate with the project's CRA steward while the issue is being fixed.

## Supported Versions

Security fixes are developed on `master` and published in the latest release of the current major version. Older releases are not routinely patched unless a published advisory says otherwise.

| Version | Supported |
| --- | --- |
| Latest release of the current major version | Yes |
| `master` | Yes |
| Older releases | No, unless noted in an advisory |

## Scope

This policy covers the eKuiper source code, official release artifacts, and official container images. Report vulnerabilities in upstream dependencies to those projects; eKuiper will update affected dependencies once a fix is available.

## CRA stewardship

This project is supported under the Linux Foundation CRA stewardship framework. Our project CRA steward is LF Projects, LLC and its policy is available at https://www.linuxfoundation.org/security. Security vulnerabilities should be reported through [GitHub private vulnerability reporting](https://github.com/lf-edge/ekuiper/security/advisories/new) which we will coordinate with our CRA steward. For actively exploited vulnerabilities or other security matters that may require CRA escalation, please email Jiyong Huang (huangjy@emqx.io) and file a private GitHub report marked `ACTIVELY EXPLOITED` or `SEVERE INCIDENT` as appropriate.
