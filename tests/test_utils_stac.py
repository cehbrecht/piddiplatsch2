# test_checksum.py
import pytest
from multiformats import multihash
from piddiplatsch.utils.stac import parse_multihash_hex

def test_parse_multihash_hex_sha256():
    mh_hex = "12205994471abb01112afcc18159f6cc74b4f511b99806da59b3caf5a9c173cacfc5"
    checksum_type, checksum_hex = parse_multihash_hex(mh_hex)
    
    assert checksum_type == "sha2-256"
    assert checksum_hex == "5994471abb01112afcc18159f6cc74b4f511b99806da59b3caf5a9c173cacfc5"

@pytest.mark.parametrize("digest_bytes,code_name", [
    (b"hello world", "sha1"),
    (b"another test", "sha2-256"),
    (b"123456", "sha2-512"),
])
def test_parse_multihash_various(digest_bytes, code_name):
    # Encode a multihash
    mh_hex = multihash.digest(digest_bytes, code_name).hex()

    # Parse
    ctype, chex = parse_multihash_hex(mh_hex)

    assert ctype == code_name
    # assert chex == digest_bytes.hex()
