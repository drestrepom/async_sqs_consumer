MESSAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "task": {"type": "string"},
        "id": {"type": "string"},
        "args": {"type": "array"},
        "kwargs": {"type": "object"},
        "retries": {"type": "number"},
        "eta": {"type": "string"},
        "expires": {"type": "string"},
    },
    "required": ["task", "id"],
}
