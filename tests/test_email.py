from unittest.mock import patch, MagicMock
from src.utils import smtp_send_mail

@patch('src.utils.os.getenv')
@patch('src.utils.smtplib.SMTP')
def test_smtp_send_mail_success(mock_smtp, mock_getenv):
    mock_getenv.side_effect = lambda key: {
        'gmail_user': 'test@example.com',
        'gmail_password': 'password',
        'email_to': 'dest@example.com'
    }.get(key)

    mock_server = MagicMock()
    mock_smtp.return_value = mock_server

    result = smtp_send_mail("Test Subject", "Test Body")

    assert result is True



@patch('src.utils.os.getenv')
@patch('src.utils.smtplib.SMTP')
def test_duplicate_email_not_sent(mock_smtp, mock_getenv):
    # test duplicate email
    mock_getenv.side_effect = lambda key: {
        'gmail_user': 'test@example.com',
        'gmail_password': 'password',
        'email_to': 'dest@example.com'
    }.get(key)

    mock_server = MagicMock()
    mock_smtp.return_value = mock_server

    subject = "Duplicate"
    body = "Same body"

    result1 = smtp_send_mail(subject, body)
    result2 = smtp_send_mail(subject, body)

    assert result1 is True
    assert result2 is False
