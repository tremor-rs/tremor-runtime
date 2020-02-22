# Security

The tremor project follows strict coding practices that help to reduce the incidence,
surface and likelihood of direct or indirect security risks to users of the software.

Specifically:

  * Tremor favors safe over unsafe rust code.
    * Safe code is generally considered the better option
    * Unless, performance critical concerns on the hot path suggest otherwise
    * Over time, unsafe code should be displaced with safe code
  * Tremor is conservative on matters of code health.
    * Clippy is pedantic mode is mandated for all rust code
    * Property based testing, model-based testing and fuzz-testing are used
    * Additional audits for code quality are in force
  * Static analysis
    * Tremor analyses external library dependencies for all direct and indirect dependencies
    * Tremor logs and reports all LICENSE, CVE and other violations both in our CI code and using other tools
    * Additional dynamic and static analysis methods can be added to broaden/deepen coverage


# Non Recommendation

We do *not* recommend running tremor outside of corporate firewalls at this time.

Although every care is taken to ensure there are no security flaws within the code-base
tremor, to date, has been designed with deployment in secure, well-defended environments
with active intrusion detection and defenses run by security professionals.


# Recommendation

We do recommend running tremor in secured environments following the best practices of
the organization and deployment platform. For example, the security blueprints for deploying
secure services to Amazon's infrastructure should be followed when deploying to AWS. The
security blueprints for the Google Cloud Platform should be followed when deploying to GCP.

Where tremor is deployed into bespoke bare metal data centers, tremor should be treated as
software that is not secure in and of itself. A secured environment should be provided for
it to run within.

# Future

Contributions to tremor security are very welcome, highly encouraged and we would be
delighted to accept contributions that move our security roadmap priority.

# Disclosing Security Issues

Safety is a core principle of Tremor, and one of the prime reasons why we adopted the
Rust Programming language to write Tremor. To that end, we would like to ensure that
Tremor has a secure implementation with no inherent security issues.

Thank you for taking the time to responsibly disclose any issues you find.

All security bugs in the tremor project should be reported by email to <a href="mailto:opensource@wayfair.com">opensource@wayfair.com</a>. This list is delivered to a small team. Your email will be acknowledged within 24 hours, and you’ll receive a more detailed response to your email within 48 hours indicating the next steps in handling your report. 

If you would like, you can encrypt your report using a public key. This key can be requested from the security
team via direct email, or through contacting us on our slack community and requesting same.

Be sure to use a descriptive subject line in email to avoid having your report missed. After the initial reply
to your report, the security team will endeavor to keep you informed of the progress being made towards a fix
and full announcement.

## Public keys for disclosures


If you have not received a reply to your email within 48 hours, or have not heard from the security team for the past five days, there are a few steps you can take (in order):

Contact the current security coordinator (Darach Ennis) directly [pgp key](https://pgp.mit.edu/pks/lookup?op=get&search=0x962FAC01B6989EBB).
Contact the back-up contact (Heinz Gies) directly [pgp key](https://keys.openpgp.org/vks/v1/by-fingerprint/71C9D7794FCEAC9D77AC4F6FE21BB9BD3F38481E).

Post a friendly reminder on the community slack.

Please note that discussion forums are public areas. When escalating in these venues, please do not
discuss your issue. Simply say that you’re trying to get a hold of someone from the security team.
