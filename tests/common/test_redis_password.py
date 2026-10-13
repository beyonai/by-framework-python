import os
import secrets
import unittest
from unittest.mock import patch

from by_framework.common import redis_password
from by_framework.common.exceptions import RedisConnectionError
from by_framework.common.redis_client import init_redis
from by_framework.common.redis_password import resolve_redis_password

_PUBLIC_EXPONENT = 65537
_KEY_LENGTH = redis_password._KEY_LENGTH
# PKCS#1 v1.5: 11 bytes of overhead per block.
_MAX_CHUNK = _KEY_LENGTH - 11


def _encrypt_block(chunk: bytes, padding: bytes = b"") -> bytes:
    if not padding:
        padding = bytes(
            secrets.randbelow(255) + 1 for _ in range(_KEY_LENGTH - 3 - len(chunk))
        )
    em = b"\x00\x02" + padding + b"\x00" + chunk
    c = pow(int.from_bytes(em, "big"), _PUBLIC_EXPONENT, redis_password._MODULUS)
    return c.to_bytes(_KEY_LENGTH, "big")


def _encrypt(plaintext: str) -> str:
    """Produce the byclaw-be {cipher} format: hex of concatenated RSA
    PKCS#1 v1.5 blocks."""
    data = plaintext.encode("utf-8")
    blocks = [
        _encrypt_block(data[i : i + _MAX_CHUNK])
        for i in range(0, len(data), _MAX_CHUNK)
    ]
    return "{cipher}" + b"".join(blocks).hex()


class TestResolveRedisPassword(unittest.TestCase):

    def test_plain_values_pass_through_unchanged(self):
        for value in (None, "", "secret", " {cipher}leading-space", "cipher}x"):
            self.assertEqual(resolve_redis_password(value), value)

    def test_decrypts_single_block(self):
        self.assertEqual(resolve_redis_password(_encrypt("s3cr3t!")), "s3cr3t!")

    def test_accepts_uppercase_hex(self):
        cipher = _encrypt("s3cr3t!")
        upper = "{cipher}" + cipher[len("{cipher}") :].upper()
        self.assertEqual(resolve_redis_password(upper), "s3cr3t!")

    def test_decrypts_multiple_blocks(self):
        plaintext = "p" * (_MAX_CHUNK * 2 + 5)
        self.assertEqual(resolve_redis_password(_encrypt(plaintext)), plaintext)

    def test_decrypts_non_ascii_plaintext(self):
        self.assertEqual(resolve_redis_password(_encrypt("密码-ü")), "密码-ü")

    def test_rejects_odd_length_hex(self):
        cipher = _encrypt("secret")
        with self.assertRaises(ValueError):
            resolve_redis_password(cipher[:-1])

    def test_rejects_bad_padding(self):
        bogus = "{cipher}" + (b"\x01" * _KEY_LENGTH).hex()
        with self.assertRaises(ValueError):
            resolve_redis_password(bogus)

    def test_rejects_short_padding(self):
        # The JDK requires at least 8 padding bytes; 7 must be rejected.
        block = _encrypt_block(b"x" * (_KEY_LENGTH - 10), padding=b"\x01" * 7)
        with self.assertRaises(ValueError):
            resolve_redis_password("{cipher}" + block.hex())

    def test_rejects_value_larger_than_modulus(self):
        with self.assertRaises(ValueError):
            resolve_redis_password("{cipher}" + "ff" * _KEY_LENGTH)

    def test_error_message_does_not_leak_value(self):
        bogus = "{cipher}" + "ab" * _KEY_LENGTH
        with self.assertRaises(ValueError) as ctx:
            resolve_redis_password(bogus)
        self.assertNotIn("abab", str(ctx.exception))


class TestInitRedisResolvesCipherPassword(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        import by_framework.common.redis_client as rc

        rc._redis_client = None

    async def asyncTearDown(self):
        import by_framework.common.redis_client as rc

        rc._redis_client = None

    async def test_env_cipher_password_is_decrypted(self):
        from by_framework.common.config import RedisConfig

        with patch.dict(os.environ, {"REDIS_PASSWORD": _encrypt("env-secret")}):
            config = RedisConfig.from_env()
        with patch("by_framework.common.redis_client.Redis") as mock_redis_cls:
            init_redis(config=config)
        self.assertEqual(mock_redis_cls.call_args.kwargs["password"], "env-secret")

    async def test_explicit_cipher_password_is_decrypted(self):
        with patch("by_framework.common.redis_client.Redis") as mock_redis_cls:
            init_redis(password=_encrypt("kwarg-secret"))
        self.assertEqual(mock_redis_cls.call_args.kwargs["password"], "kwarg-secret")

    async def test_plain_password_passes_through(self):
        with patch("by_framework.common.redis_client.Redis") as mock_redis_cls:
            init_redis(password="plain-secret")
        self.assertEqual(mock_redis_cls.call_args.kwargs["password"], "plain-secret")

    async def test_undecryptable_cipher_password_fails_loudly(self):
        with patch("by_framework.common.redis_client.Redis") as mock_redis_cls:
            with self.assertRaises(RedisConnectionError):
                init_redis(password="{cipher}zz")
        mock_redis_cls.assert_not_called()
