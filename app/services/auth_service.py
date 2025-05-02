import os
import logging

from config.settings import APP_PASSWORD, APP_PASSWORD_WALL_ENABLED

logger = logging.getLogger(__name__)


class AuthService:
    """Service for handling authentication"""

    @staticmethod
    def is_password_protection_enabled():
        """Check if password protection is enabled

        Returns:
            bool: True if password protection is enabled, False otherwise
        """
        enabled = APP_PASSWORD_WALL_ENABLED.lower()
        return enabled in ("true", "1", "yes")

    @staticmethod
    def validate_password(input_password):
        """Validate the input password against the app password

        Args:
            input_password (str): The password entered by the user

        Returns:
            bool: True if passwords match, False otherwise
        """
        app_password = APP_PASSWORD
        if not app_password:
            logger.warning(
                "APP_PASSWORD is not set, but password protection is enabled"
            )
            return False

        return input_password == app_password
