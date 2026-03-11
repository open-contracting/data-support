import json
from collections import defaultdict
from pathlib import Path

import click

from cli.dot import generate_erd
from cli.util import ref_name

# Always include in ERD (never prune even if frequently referenced).
NEVER_PRUNE = set()


def _collect_all_refs(obj):
    """Yield all $ref schema names from a JSON structure."""
    if isinstance(obj, dict):
        if "$ref" in obj:
            yield ref_name(obj)
        for v in obj.values():
            yield from _collect_all_refs(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _collect_all_refs(v)


def _build_reference_map(all_schemas):
    """
    Build forward reference map and counts in a single pass.

    Returns (ref_counts, refs_from) where:
    - ref_counts: {schema_name: int} — how many times each schema is referenced
    - refs_from: {schema_name: set} — which schemas each schema references
    """
    ref_counts = defaultdict(int)
    refs_from = {}

    for name, schema in all_schemas.items():
        refs = set()
        for ref in _collect_all_refs(schema):
            ref_counts[ref] += 1
            refs.add(ref)
        refs_from[name] = refs

    return ref_counts, refs_from


def _find_orphaned_types(all_schemas, refs_from, excluded_types):
    """Find schemas only referenced by excluded types (recursively)."""
    # Build reverse reference map: schema -> set of schemas that reference it
    referenced_by = defaultdict(set)
    for name, refs in refs_from.items():
        for ref in refs:
            referenced_by[ref].add(name)

    orphaned = set()

    # Iteratively find orphans
    while True:
        new_orphans = set()
        for name in all_schemas:
            if name in excluded_types or name in orphaned:
                continue
            # Check if all references to this schema come from excluded/orphaned types
            referrers = referenced_by.get(name, set())
            if referrers and referrers <= (excluded_types | orphaned):
                new_orphans.add(name)

        if not new_orphans:
            break
        orphaned |= new_orphans

    return orphaned


def register_command(cli):
    @cli.command()
    @click.argument("schema_file", type=click.Path(exists=True))
    @click.option("-o", "--output", type=click.Path(), help="Output file (default: stdout)")
    @click.option("--no-properties", is_flag=True, help="Hide properties, show only relationships")
    @click.option(
        "--basic-threshold",
        type=int,
        default=10,
        help="Schemas referenced at least this many times are treated as basic types and omitted",
    )
    @click.option(
        "--max-properties",
        type=int,
        default=10,
        help="Maximum properties to show per schema (0 for unlimited)",
    )
    @click.option(
        "--basic-types-output",
        type=click.Path(),
        help="Output file for a second ERD showing only the omitted basic types",
    )
    def schema_erd(schema_file, output, no_properties, basic_threshold, max_properties, basic_types_output):
        """
        Generate an ERD diagram from a deduplicated JSON Schema file.

        Reads the output of deduplicate-schema and generates a visual diagram
        showing schemas, their properties, and relationships.

        Frequently-referenced schemas (e.g., base.MultiLang, base.ValueWithTax) are
        treated as "basic types" and omitted from relationships to reduce clutter.
        """
        with open(schema_file) as f:
            data = json.load(f)

        schemas = data.get("schemas", {})
        defs = data.get("$defs", {})
        all_schemas = {**defs, **schemas}

        # Build reference map in a single pass
        ref_counts, refs_from = _build_reference_map(all_schemas)

        # Identify basic types (referenced more than threshold times)
        basic_types = {
            name for name, count in ref_counts.items() if count >= basic_threshold and name not in NEVER_PRUNE
        }

        if basic_types:
            click.echo(f"Omitting {len(basic_types)} basic types (referenced >={basic_threshold} times):", err=True)
            for name in sorted(basic_types):
                click.echo(f"  - {name} ({ref_counts[name]} refs)", err=True)

        # Find orphaned types (only referenced by basic types)
        orphaned_types = _find_orphaned_types(all_schemas, refs_from, basic_types)
        if orphaned_types:
            click.echo(f"Omitting {len(orphaned_types)} orphaned types (only referenced by basic types):", err=True)
            for name in sorted(orphaned_types):
                click.echo(f"  - {name}", err=True)

        # Find unreferenced types by checking reachability from main procedure schemas
        main_schemas = {name for name in schemas if "Procedure" in name}
        reachable = set(main_schemas)

        # Iteratively find all reachable schemas using precomputed refs_from
        to_process = set(main_schemas)
        while to_process:
            current = to_process.pop()
            for ref in refs_from.get(current, set()):
                if ref not in reachable and ref in all_schemas:
                    reachable.add(ref)
                    to_process.add(ref)

        # Unreferenced = exists but not reachable from main schemas
        unreferenced = set(all_schemas.keys()) - reachable - basic_types - orphaned_types
        if unreferenced:
            click.echo(
                f"Omitting {len(unreferenced)} unreferenced types (not reachable from Procedure schemas):", err=True
            )
            for name in sorted(unreferenced):
                click.echo(f"  - {name}", err=True)

        # Combine all excluded types
        excluded_types = basic_types | orphaned_types | unreferenced

        result = generate_erd(all_schemas, no_properties, excluded_types, max_properties)

        if output:
            Path(output).write_text(result)
            click.echo(f"Wrote {len(result)} bytes to {output}")
        else:
            click.echo(result)

        # Generate second ERD with only basic types if requested
        if basic_types_output and basic_types:
            # Filter to only basic types and their relationships
            basic_schemas = {name: all_schemas[name] for name in basic_types if name in all_schemas}
            # Also include orphaned types that depend on basic types
            basic_schemas.update({name: all_schemas[name] for name in orphaned_types if name in all_schemas})

            basic_result = generate_erd(basic_schemas, no_properties, set(), max_properties)

            Path(basic_types_output).write_text(basic_result)
            click.echo(f"Wrote {len(basic_result)} bytes to {basic_types_output} (basic types ERD)")
