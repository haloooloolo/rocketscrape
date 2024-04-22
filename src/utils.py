def sanitize_str(string: str) -> str:
    return string.encode('ascii', 'ignore').decode('ascii')
