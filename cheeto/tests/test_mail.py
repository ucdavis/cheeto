"""Rendering tests for the cheeto.mail Email subclasses — pure jinja/markdown,
no network. Focused on the offboarding lifecycle emails added alongside the
IAM sync notifications."""

from ..mail import AccountDeactivatedEmail, AccountOffboardingEmail


def _body(mail) -> str:
    return '\n\n'.join(mail.paragraphs())


class TestAccountOffboardingEmail:

    def _mail(self) -> AccountOffboardingEmail:
        return AccountOffboardingEmail(
            to=['jdoe@example.edu'],
            username='jdoe',
            deactivation_date='May 31, 2026',
        )

    def test_subject_header_recipients(self):
        mail = self._mail()
        assert mail.subject == 'UCD HPC: Account Scheduled for Deactivation'
        assert mail.header == 'Your account has been marked for offboarding'
        assert mail.emails == ['jdoe@example.edu']
        assert mail.ccEmails == []

    def test_body_mentions_username_and_date(self):
        body = _body(self._mail())
        assert 'jdoe' in body
        assert 'May 31, 2026' in body

    def test_requires_recipient(self):
        import pytest
        with pytest.raises(ValueError):
            AccountOffboardingEmail(username='jdoe', deactivation_date='x')


class TestAccountDeactivatedEmail:

    def _mail(self) -> AccountDeactivatedEmail:
        return AccountDeactivatedEmail(to=['jdoe@example.edu'], username='jdoe')

    def test_subject_header(self):
        mail = self._mail()
        assert mail.subject == 'UCD HPC: Account Deactivated'
        assert mail.header == 'Your account has been deactivated'

    def test_body_mentions_username(self):
        assert 'jdoe' in _body(self._mail())
