from dagster import resource
from backend.dagster_break_analytics.resources.email_notification import (
    EmailNotification,
)


@resource(config_schema=EmailNotification.to_config_schema())
def email_notification_resource(context):
    """Email notification resource configured for Dagster."""
    return EmailNotification(
        smtp_server=context.resource_config["smtp_server"],
        smtp_port=context.resource_config["smtp_port"],
        sender_email=context.resource_config["sender_email"],
        sender_password=context.resource_config["sender_password"],
        recipient_emails=context.resource_config["recipient_emails"],
    )
