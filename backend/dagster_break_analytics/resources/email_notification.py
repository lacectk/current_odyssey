from dagster import ConfigurableResource
import smtplib
from email.message import EmailMessage
from typing import List
import ssl
from dagster import get_dagster_logger


class EmailClient:
    """Stateful client for sending emails."""

    def __init__(
        self,
        smtp_server: str,
        smtp_port: int,
        sender_email: str,
        sender_password: str,
        recipient_emails: List[str],
    ):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender_email = sender_email
        self.sender_password = sender_password
        self.recipient_emails = recipient_emails
        self.logger = get_dagster_logger()

    def send_message(self, subject: str, message: str) -> None:
        """Send an email notification."""
        try:
            email = EmailMessage()
            email["Subject"] = subject
            email["From"] = self.sender_email
            email["To"] = ", ".join(self.recipient_emails)
            email.set_content(message)

            context = ssl.create_default_context()

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls(context=context)
                server.login(self.sender_email, self.sender_password)
                server.send_message(email)

            self.logger.info(f"Email notification sent: {subject}")

        except Exception as e:
            self.logger.error(f"Failed to send email notification: {str(e)}")


class EmailNotification(ConfigurableResource):
    """Email notification resource that creates an email client."""

    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    sender_email: str
    sender_password: str
    recipient_emails: List[str]

    def get_client(self) -> EmailClient:
        """Return a configured email client."""
        return EmailClient(
            smtp_server=self.smtp_server,
            smtp_port=self.smtp_port,
            sender_email=self.sender_email,
            sender_password=self.sender_password,
            recipient_emails=self.recipient_emails,
        )
