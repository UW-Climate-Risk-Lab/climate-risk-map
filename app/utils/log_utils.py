import os
import logging
import sys
from logging.handlers import RotatingFileHandler

def configure_logging(
    log_level=logging.INFO,
    log_format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    log_dir="logs"
):
    """Configure application logging
    
    Sets up logging with file and console handlers. Creates log directory
    if it doesn't exist.
    
    Args:
        log_level: Logging level (default: INFO)
        log_format: Log message format
        log_dir: Directory for log files
    """
    # Create logs directory if it doesn't exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    # Create formatter
    formatter = logging.Formatter(log_format)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear existing handlers
    root_logger.handlers = []
    
    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Add file handler with rotation
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, "climate_risk_map.log"),
        maxBytes=10485760,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # Set specific log levels for noisy modules
    # logging.getLogger("werkzeug").setLevel(logging.WARNING)
    # logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    # Log startup
    root_logger.info("Logging configured")