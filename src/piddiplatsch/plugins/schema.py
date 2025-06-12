CMIP6_ITEM_SCHEMA = {
    "type": "object",
    "required": ["id", "links", "properties", "assets"],
    "properties": {
        "id": {"type": "string"},
        "links": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["href"],
                "properties": {
                    "href": {"type": "string"}
                }
            },
        },
        # "properties": {
        #     "type": "object",
        #     "required": ["version"],
        #     "properties": {
        #         "version": {"type": "string"},
        #     },
        # },
        "assets": {
            "type": "object",
            "required": ["reference_file"],
            "properties": {
                "reference_file": {
                    "type": "object",
                    "required": ["alternate:name"],
                    "properties": {
                        "alternate:name": {"type": "string"},
                    },
                }
            },
        },
    },
}
