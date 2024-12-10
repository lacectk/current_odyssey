import unittest
from unittest.mock import patch, MagicMock
from dagster import build_init_resource_context
from backend.dagster_break_analytics.resources.email_notification import (
    EmailNotification,
)
import smtplib


class TestEmailNotification(unittest.TestCase):
    def setUp(self):
        """Set up test cases."""
        # Create init context
        self.init_context = build_init_resource_context()

        # Create the resource with configuration
        self.email_resource = EmailNotification(
            sender_email="test@example.com",
            sender_password="test_password",
            recipient_emails=["recipient1@example.com"],
        )

        # Get the client for testing
        self.email_client = self.email_resource.get_client()

        # Test data
        self.test_subject = "Test Subject"
        self.test_message = "Test Message Content"

    @patch("smtplib.SMTP")
    def test_resource_configuration(self, _):
        """Test resource configuration."""
        self.assertEqual(self.email_resource.smtp_server, "smtp.gmail.com")
        self.assertEqual(self.email_resource.smtp_port, 587)
        self.assertEqual(self.email_resource.sender_email, "test@example.com")
        self.assertEqual(self.email_resource.sender_password, "test_password")
        self.assertEqual(
            self.email_resource.recipient_emails, ["recipient1@example.com"]
        )

    @patch("smtplib.SMTP")
    def test_client_creation(self, _):
        """Test client creation and configuration."""
        client = self.email_resource.get_client()
        self.assertEqual(client.smtp_server, self.email_resource.smtp_server)
        self.assertEqual(client.smtp_port, self.email_resource.smtp_port)
        self.assertEqual(client.sender_email, self.email_resource.sender_email)
        self.assertEqual(client.sender_password, self.email_resource.sender_password)
        self.assertEqual(client.recipient_emails, self.email_resource.recipient_emails)

    @patch("smtplib.SMTP")
    def test_send_message_success(self, mock_smtp):
        """Test successful email sending using client."""
        # Set up mock SMTP instance
        mock_smtp_instance = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_smtp_instance

        # Use the client to send the message
        client = self.email_resource.get_client()
        client.send_message(subject=self.test_subject, message=self.test_message)

        # Verify SMTP calls
        mock_smtp.assert_called_once_with(client.smtp_server, client.smtp_port)
        mock_smtp_instance.starttls.assert_called_once()
        mock_smtp_instance.login.assert_called_once_with(
            client.sender_email, client.sender_password
        )

        # Verify email content
        self.assertEqual(mock_smtp_instance.send_message.call_count, 1)
        sent_email = mock_smtp_instance.send_message.call_args[0][0]
        self.assertEqual(sent_email["Subject"], self.test_subject)
        self.assertEqual(sent_email["From"], client.sender_email)
        self.assertEqual(sent_email["To"], ", ".join(client.recipient_emails))

    @patch("smtplib.SMTP")
    def test_send_message_smtp_error(self, mock_smtp):
        """Test handling of SMTP errors using client."""
        # Make SMTP raise an error
        mock_smtp_instance = MagicMock()
        mock_smtp_instance.send_message.side_effect = smtplib.SMTPException(
            "SMTP Error"
        )
