"""Redis password resolution.

Plain text values are returned unchanged; values prefixed with ``{cipher}``
use the built-in RSA cipher format shared with byclaw-be. This is a
line-for-line port of the Java SDK's ``RedisPasswordCipher`` so both SDKs
accept exactly the same ciphertexts.
"""

from typing import Optional

CIPHER_PREFIX = "{cipher}"

_MODULUS = int(
    "118249911269313777353369205468358971445904616358074252669429944094950609"
    "311848727388859640174096832607664732279928019002795333396858212446930193"
    "162882569448161371673123891912144968110489981164388431260818873249786366"
    "362488910362896712606417158347288417834035704864053434682010900207183838"
    "254091105621956825021"
)

_PRIVATE_KEY = int(
    "486373286567224954199523979218622218639212176106359698331805479808884847"
    "431251704456124091815761206611869710749307975683865756297314490245121120"
    "420321732982564126362643243208709861523940153746233446702040720249396157"
    "848350707068412091346394073481926826790160613505333343349449539446344171"
    "83180191532927602945"
)

# Java: privateKey.getModulus().bitLength() / 8
_KEY_LENGTH = _MODULUS.bit_length() // 8

# The JDK's PKCS#1 v1.5 unpadding requires at least 8 bytes of padding.
_MIN_PADDING_LENGTH = 8


def resolve_redis_password(value: Optional[str]) -> Optional[str]:
    """Return ``value`` unchanged unless it starts with ``{cipher}``, in which
    case decrypt the remainder.

    Raises:
        ValueError: The ``{cipher}`` value could not be decrypted. The message
            never includes the ciphertext or any decrypted bytes.
    """
    if not value or not value.startswith(CIPHER_PREFIX):
        return value
    return _decrypt(value[len(CIPHER_PREFIX) :])


def _decrypt(text: str) -> str:
    try:
        ascii_bytes = text.encode("utf-8")
        # Java passes input.length() (UTF-16 units) as the length; for the
        # ASCII hex input this format uses, it equals the byte length.
        bcd = _ascii_to_bcd(ascii_bytes, len(text))
        plaintext = []
        for block in _split_array(bcd, _KEY_LENGTH):
            plaintext.append(_rsa_decrypt_block(block).decode("utf-8", "replace"))
        return "".join(plaintext)
    except Exception as e:
        raise ValueError("Failed to decrypt REDIS_PASSWORD {cipher} value") from e


def _rsa_decrypt_block(block: bytes) -> bytes:
    """Equivalent of the JDK's ``Cipher.getInstance("RSA")`` decrypt, i.e.
    RSA/ECB/PKCS1Padding, for a single block."""
    c = int.from_bytes(block, "big")
    if c >= _MODULUS:
        raise ValueError("Message is larger than modulus")
    em = pow(c, _PRIVATE_KEY, _MODULUS).to_bytes(_KEY_LENGTH, "big")
    if em[0] != 0x00 or em[1] != 0x02:
        raise ValueError("Decryption error")
    separator = em.find(b"\x00", 2)
    if separator < 0 or separator - 2 < _MIN_PADDING_LENGTH:
        raise ValueError("Decryption error")
    return em[separator + 1 :]


def _ascii_to_bcd(ascii_bytes: bytes, length: int) -> bytes:
    bcd = bytearray(length // 2)
    j = 0
    for i in range((length + 1) // 2):
        # Like Java's byte[] indexing, odd-length input runs past the end and
        # raises IndexError here.
        high = _asc_to_bcd(ascii_bytes[j])
        j += 1
        low = 0x00 if j >= length else _asc_to_bcd(ascii_bytes[j])
        j += 1
        bcd[i] = (low + (high << 4)) & 0xFF
    return bytes(bcd)


def _asc_to_bcd(asc: int) -> int:
    if ord("0") <= asc <= ord("9"):
        return asc - ord("0")
    if ord("A") <= asc <= ord("F"):
        return asc - ord("A") + 10
    if ord("a") <= asc <= ord("f"):
        return asc - ord("a") + 10
    return (asc - 48) & 0xFF


def _split_array(data: bytes, length: int) -> list[bytes]:
    # A short trailing block is right-padded with zeros, as in Java.
    return [
        data[i : i + length].ljust(length, b"\x00")
        for i in range(0, len(data), length)
    ]
