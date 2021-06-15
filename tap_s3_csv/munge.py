import singer

LOGGER = singer.get_logger()

def munge(rec, munge_config):
    out = {}
    for c in munge_config["columns"]:
        out[c["name"]] = _process_column(rec, c)
    return out


def _process_column(rec, col_config):
    name = col_config["name"]
    steps = col_config.get("steps", [])
    # if no steps defined, fall back to pass-through
    if len(steps) == 0:
        steps = [{"method": "pass", "value": name}]

    for step in steps:
        method = step.get("method", "pass").lower()
        if method == "constant":
            if "value" in step:
                return step["value"]
            else:
                LOGGER.warning(f"No constant value found for {name}")
                return ""
        elif method == "cat":
            pass
        elif method == "pass":
            if step.get("value", None) in rec:
                return rec[step["value"]]
            else:
                LOGGER.warning(f"Pass-through source column {step['value']} for {name} not found in record")
                return ""
    return None
