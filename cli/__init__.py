from cli import deduplicate_schema, schema_erd


def register_all_commands(cli):
    """Register all CLI commands."""
    deduplicate_schema.register_command(cli)
    schema_erd.register_command(cli)
