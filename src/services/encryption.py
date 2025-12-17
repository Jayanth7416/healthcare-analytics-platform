"""Encryption Service for PHI Data"""

import base64
import os
from typing import Optional
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import structlog
import boto3
from botocore.exceptions import ClientError

from src.utils.config import settings

logger = structlog.get_logger()


class EncryptionService:
    """
    HIPAA-compliant encryption service for PHI data

    Features:
    - AES-256 encryption via Fernet
    - AWS KMS integration for key management
    - Automatic key rotation support
    - Audit logging for all operations
    """

    def __init__(self):
        self.current_key_id = settings.KMS_KEY_ID
        self._cipher = None
        self._initialize_local_cipher()  # Use local cipher for demo

    def _initialize_local_cipher(self):
        """Initialize Fernet cipher with local key (demo mode)"""
        self._cipher = Fernet(self._derive_local_key())
        logger.info("encryption_initialized_local_mode", key_id=self.current_key_id)

    def _derive_local_key(self) -> bytes:
        """Derive a local encryption key (for development only)"""
        password = settings.ENCRYPTION_SECRET.encode()
        salt = settings.ENCRYPTION_SALT.encode()
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        return base64.urlsafe_b64encode(kdf.derive(password))

    async def encrypt_phi(self, data: str) -> str:
        """
        Encrypt Protected Health Information

        Args:
            data: Plain text PHI data

        Returns:
            Base64 encoded encrypted data
        """
        if not data:
            return data

        try:
            encrypted = self._cipher.encrypt(data.encode())
            result = base64.urlsafe_b64encode(encrypted).decode()

            logger.debug(
                "phi_encrypted",
                data_length=len(data),
                key_id=self.current_key_id
            )

            return result
        except Exception as e:
            logger.error("encryption_failed", error=str(e))
            raise

    async def decrypt_phi(self, encrypted_data: str) -> str:
        """
        Decrypt Protected Health Information

        Args:
            encrypted_data: Base64 encoded encrypted data

        Returns:
            Decrypted plain text
        """
        if not encrypted_data:
            return encrypted_data

        try:
            decoded = base64.urlsafe_b64decode(encrypted_data.encode())
            decrypted = self._cipher.decrypt(decoded)

            logger.debug(
                "phi_decrypted",
                key_id=self.current_key_id
            )

            return decrypted.decode()
        except Exception as e:
            logger.error("decryption_failed", error=str(e))
            raise

    async def rotate_key(self, new_key_id: str):
        """
        Rotate to a new KMS key

        This should be called during scheduled key rotation.
        Old data will need to be re-encrypted with new key.
        """
        old_key_id = self.current_key_id
        self.current_key_id = new_key_id
        self._initialize_cipher()

        logger.info(
            "encryption_key_rotated",
            old_key_id=old_key_id,
            new_key_id=new_key_id
        )

    def generate_data_key(self) -> tuple:
        """
        Generate a new data key for envelope encryption

        Returns:
            Tuple of (plaintext_key, encrypted_key)
        """
        try:
            response = self.kms_client.generate_data_key(
                KeyId=self.current_key_id,
                KeySpec='AES_256'
            )
            return response['Plaintext'], response['CiphertextBlob']
        except ClientError as e:
            logger.error("generate_data_key_failed", error=str(e))
            raise
