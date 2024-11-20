from dagster import ConfigurableResource
import smtplib
from email.message import EmailMessage
from typing import List
import ssl
import os


class EmailNotification(ConfigurableResource):
    """Email notification resource using EmailMessage for modern email handling."""

    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    sender_email: str = os.getenv("SENDER_EMAIL")
    sender_password: str = os.getenv("SENDER_EMAIL_PASSWORD")
    recipient_emails: List[str] = os.getenv("RECIPIENT_EMAILS").split(",")

    def send_message(self, subject: str, message: str) -> None:
        """
        Send an email notification using EmailMessage.

        Args:
            subject: Email subject line
            message: Email body content
        """
        try:
            # Create EmailMessage object
            email = EmailMessage()

            # Set basic headers
            email["Subject"] = subject
            email["From"] = self.sender_email
            email["To"] = ", ".join(self.recipient_emails)

            # Set content (automatically handles content-type)
            email.set_content(message)

            # Create secure context
            context = ssl.create_default_context()

            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls(context=context)  # Secure the connection
                server.login(self.sender_email, self.sender_password)
                server.send_message(email)

            self.log.info(f"Email notification sent: {subject}")

        except Exception as e:
            self.log.error(f"Failed to send email notification: {str(e)}")
