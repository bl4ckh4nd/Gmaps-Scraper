"""Encryption utilities for secure storage of sensitive data."""

import os
import logging
from pathlib import Path
from typing import Optional
from cryptography.fernet import Fernet


logger = logging.getLogger(__name__)


class EncryptionError(Exception):
    """Raised when encryption/decryption operations fail."""
    pass


class SecureStorage:
    """Handles encryption and decryption of sensitive data using Fernet symmetric encryption."""

    def __init__(self, key_path: Optional[Path] = None):
        """Initialize secure storage.

        Args:
            key_path: Path to encryption key file. If None, uses environment variable
                     SCRAPER_ENCRYPTION_KEY or default path web/database/.encryption_key
        """
        self.key_path = key_path or Path('web/database/.encryption_key')
        self._key: Optional[bytes] = None
        self._cipher: Optional[Fernet] = None

    def _load_or_create_key(self) -> bytes:
        """Load encryption key from file or environment, or create new one.

        Returns:
            Encryption key as bytes

        Raises:
            EncryptionError: If key cannot be loaded or created
        """
        # Try environment variable first
        env_key = os.getenv('SCRAPER_ENCRYPTION_KEY')
        if env_key:
            try:
                return env_key.encode('utf-8')
            except Exception as e:
                logger.warning(f"Invalid encryption key in environment variable: {e}")

        # Try loading from file
        if self.key_path.exists():
            try:
                with open(self.key_path, 'rb') as f:
                    key = f.read()
                    # Validate key format
                    Fernet(key)  # Will raise if invalid
                    logger.info(f"Loaded encryption key from {self.key_path}")
                    return key
            except Exception as e:
                logger.error(f"Failed to load encryption key from {self.key_path}: {e}")
                raise EncryptionError(f"Invalid encryption key file: {e}")

        # Generate new key
        try:
            key = Fernet.generate_key()

            # Ensure directory exists
            self.key_path.parent.mkdir(parents=True, exist_ok=True)

            # Write key to file
            with open(self.key_path, 'wb') as f:
                f.write(key)

            # Set restrictive permissions (owner read/write only)
            try:
                os.chmod(self.key_path, 0o600)
            except Exception as e:
                logger.warning(f"Could not set file permissions on {self.key_path}: {e}")

            logger.info(f"Generated new encryption key at {self.key_path}")
            return key

        except Exception as e:
            logger.error(f"Failed to generate encryption key: {e}")
            raise EncryptionError(f"Could not create encryption key: {e}")

    def _get_cipher(self) -> Fernet:
        """Get or initialize cipher instance.

        Returns:
            Fernet cipher instance

        Raises:
            EncryptionError: If cipher cannot be initialized
        """
        if self._cipher is None:
            if self._key is None:
                self._key = self._load_or_create_key()
            try:
                self._cipher = Fernet(self._key)
            except Exception as e:
                logger.error(f"Failed to initialize cipher: {e}")
                raise EncryptionError(f"Cipher initialization failed: {e}")

        return self._cipher

    def encrypt(self, plaintext: str) -> str:
        """Encrypt a string value.

        Args:
            plaintext: String to encrypt

        Returns:
            Base64-encoded encrypted string

        Raises:
            EncryptionError: If encryption fails
        """
        if not plaintext:
            return ""

        try:
            cipher = self._get_cipher()
            encrypted_bytes = cipher.encrypt(plaintext.encode('utf-8'))
            return encrypted_bytes.decode('utf-8')
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            raise EncryptionError(f"Failed to encrypt data: {e}")

    def decrypt(self, ciphertext: str) -> str:
        """Decrypt an encrypted string value.

        Args:
            ciphertext: Base64-encoded encrypted string

        Returns:
            Decrypted plaintext string

        Raises:
            EncryptionError: If decryption fails
        """
        if not ciphertext:
            return ""

        try:
            cipher = self._get_cipher()
            decrypted_bytes = cipher.decrypt(ciphertext.encode('utf-8'))
            return decrypted_bytes.decode('utf-8')
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            raise EncryptionError(f"Failed to decrypt data: {e}")

    def rotate_key(self, new_key_path: Optional[Path] = None) -> None:
        """Rotate encryption key.

        Warning: This will invalidate all previously encrypted data unless
        you manually re-encrypt it with the new key.

        Args:
            new_key_path: Path for new key file. If None, overwrites current key file.

        Raises:
            EncryptionError: If key rotation fails
        """
        target_path = new_key_path or self.key_path

        try:
            # Generate new key
            new_key = Fernet.generate_key()

            # Write to file
            target_path.parent.mkdir(parents=True, exist_ok=True)
            with open(target_path, 'wb') as f:
                f.write(new_key)

            # Set permissions
            try:
                os.chmod(target_path, 0o600)
            except Exception as e:
                logger.warning(f"Could not set file permissions: {e}")

            # Update instance
            self._key = new_key
            self._cipher = None  # Force re-initialization

            logger.info(f"Encryption key rotated successfully at {target_path}")

        except Exception as e:
            logger.error(f"Key rotation failed: {e}")
            raise EncryptionError(f"Failed to rotate encryption key: {e}")
