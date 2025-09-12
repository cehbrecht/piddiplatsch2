def detect_checksum_type(checksum: str) -> str:
    """Guess checksum algorithm from hex length or multihash prefix."""
    length = len(checksum)

    if checksum.startswith("1220") and length == 68:
        return "SHA256-MULTIHASH"
    if length == 32:
        return "MD5"
    if length == 40:
        return "SHA1"
    if length == 64:
        return "SHA256"
    if length == 128:
        return "SHA512"

    # Unknown checksum type
    return "UNKNOWN"
