import functools
import json
from collections import defaultdict
from pathlib import Path

import click

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


def _safe_name(name):
    """Convert schema name to safe identifier for diagrams."""
    return name.replace("-", "_").replace(".", "_").replace(" ", "_").replace("|", "_or_")


def _get_type_str(prop_schema):
    """Get a simple type string for a property."""
    if "$ref" in prop_schema:
        return ref_name(prop_schema)
    if "type" in prop_schema:
        t = prop_schema["type"]
        if t == "array" and "items" in prop_schema:
            items = prop_schema["items"]
            if "$ref" in items:
                return ref_name(items) + "[]"
            # Handle anyOf/oneOf in array items
            for key in ("anyOf", "oneOf"):
                if key in items:
                    types = [
                        ref_name(opt)
                        for opt in _iter_union_options(items[key])
                        if isinstance(opt, dict) and "$ref" in opt
                    ]
                    if types:
                        return "|".join(types) + "[]"
            return items.get("type", "any") + "[]"
        return t
    # Handle anyOf/oneOf at property level
    for key in ("anyOf", "oneOf"):
        if key in prop_schema:
            types = [
                ref_name(opt)
                for opt in _iter_union_options(prop_schema[key])
                if isinstance(opt, dict) and "$ref" in opt
            ]
            if types:
                return "|".join(types)
    if "allOf" in prop_schema:
        return "object"
    return "any"


def _extract_relationships(name, schema):
    """Extract relationships from a schema."""
    relationships = []

    # Check allOf for inheritance
    if "allOf" in schema:
        for item in schema["allOf"]:
            if "$ref" in item:
                parent = ref_name(item)
                relationships.append((name, parent, "inherits", ""))
            if "properties" in item:
                relationships.extend(_extract_prop_relationships(name, item["properties"]))

    # Check direct properties
    if "properties" in schema:
        relationships.extend(_extract_prop_relationships(name, schema["properties"]))

    return relationships


def _extract_prop_relationships(name, properties):
    """Extract relationships from properties."""
    relationships = []
    for prop_name, prop_schema in properties.items():
        refs = _extract_refs_from_prop(prop_schema)
        for target, is_array in refs:
            rel_type = "||--o{" if is_array else "||--o|"
            relationships.append((name, target, rel_type, prop_name))
    return relationships


def _iter_union_options(union_value):
    """Iterate over anyOf/oneOf options, handling both list and dict forms."""
    if isinstance(union_value, list):
        yield from union_value
    elif isinstance(union_value, dict):
        yield from union_value.values()


def _extract_refs_from_prop(prop_schema):
    """Extract all $ref targets from a property schema, handling anyOf/oneOf/allOf."""
    refs = []

    if "$ref" in prop_schema:
        refs.append((ref_name(prop_schema), False))

    # Handle array items
    if "items" in prop_schema:
        items = prop_schema["items"]
        if "$ref" in items:
            refs.append((ref_name(items), True))
        # Handle anyOf/oneOf in array items
        for key in ("anyOf", "oneOf", "allOf"):
            if key in items:
                refs.extend(
                    (ref_name(option), True)
                    for option in _iter_union_options(items[key])
                    if isinstance(option, dict) and "$ref" in option
                )

    # Handle anyOf/oneOf/allOf at property level
    for key in ("anyOf", "oneOf", "allOf"):
        if key in prop_schema:
            refs.extend(
                (ref_name(option), False)
                for option in _iter_union_options(prop_schema[key])
                if isinstance(option, dict) and "$ref" in option
            )

    return refs


@functools.cache
def _get_color_key(name):
    """
    Extract color key from schema name for coloring.

    Looks for known type strings in the name. If none found, returns 'Other'.

    Examples:
        'ocp.AwardBase' -> 'Award'
        'embed.dgf.Award' -> 'Award'
        'ocp.EnglishBidMixin' -> 'Mixin'
        'dgf-english.DgfEnglishProcedure' -> 'Procedure'
        'base.ContactPoint' -> 'Other'

    """
    # Known type strings to look for (in priority order - Mixin before Bid since BidMixin exists)
    known_types = (
        "Procedure",
        "Award",
        "Contract",
        "Mixin",
        "Bid",
        "Item",
        "Props",
        "Organization",
    )

    # Check only the last segment (after the last dot)
    last_segment = name.rsplit(".", 1)[-1]
    for type_str in known_types:
        if type_str in last_segment:
            return type_str

    if any(kind in last_segment for kind in ("Dgf", "RenewablesMultiAwards", "RailwayCargo", "Subsoil", "Timber")):
        return "Kind"

    return "Other"


def _generate_color_map(all_schemas, excluded):
    """Generate a color map for schema types (based on last dot section)."""
    # Colorblind-friendly palette based on Okabe-Ito (pastel variants for readability)
    # https://jfly.uni-koeln.de/color/
    colors = [
        "#56B4E9",  # sky blue
        "#E69F00",  # orange
        "#009E73",  # bluish green
        "#F0E442",  # yellow
        "#0072B2",  # blue
        "#D55E00",  # vermilion
        "#CC79A7",  # reddish purple
        "#999999",  # gray
        "#88CCEE",  # cyan
        "#DDCC77",  # sand
    ]

    color_keys = sorted({_get_color_key(name) for name in all_schemas if name not in excluded})
    return {key: colors[i % len(colors)] for i, key in enumerate(color_keys)}


def generate_dot_erd(all_schemas, no_properties, excluded=None, max_properties=10):
    """Generate Graphviz DOT format."""
    excluded = excluded or set()
    color_map = _generate_color_map(all_schemas, excluded)

    lines = [
        "digraph ERD {",
        "    rankdir=LR;",
        "    node [shape=record, fontname=Helvetica, fontsize=10, style=filled];",
        "    edge [fontname=Helvetica, fontsize=9];",
        "",
    ]

    # Collect all relationships
    all_rels = []
    for name, schema in all_schemas.items():
        if isinstance(schema, dict):
            all_rels.extend(_extract_relationships(name, schema))

    # Generate nodes
    for name, schema in sorted(all_schemas.items()):
        if not isinstance(schema, dict) or name in excluded:
            continue

        safe_name = _safe_name(name)
        prefix = _get_color_key(name)
        fillcolor = color_map.get(prefix, "#FFFFFF")

        if no_properties:
            lines.append(f'    {safe_name} [label="{name}", fillcolor="{fillcolor}"];')
        else:
            props = []
            if "allOf" in schema:
                for item in schema["allOf"]:
                    if "properties" in item:
                        props.extend(item["properties"].items())
            elif "properties" in schema:
                props = list(schema["properties"].items())

            prop_lines = [name, ""]
            display_props = props if max_properties == 0 else props[:max_properties]
            for prop_name, prop_schema in display_props:
                type_str = _get_type_str(prop_schema)
                prop_lines.append(f"{prop_name}: {type_str}")
            if max_properties > 0 and len(props) > max_properties:
                prop_lines.append(f"... +{len(props) - max_properties} more")

            label = "\\l".join(prop_lines) + "\\l"
            lines.append(f'    {safe_name} [label="{{{label}}}", fillcolor="{fillcolor}"];')

    lines.append("")

    # Generate edges
    seen_rels = set()
    for source, target, rel_type, label in all_rels:
        # Skip relationships to/from excluded types (except inheritance)
        if target in excluded and rel_type != "inherits":
            continue
        if source in excluded:
            continue

        safe_source = _safe_name(source)
        safe_target = _safe_name(target)

        if rel_type == "inherits":
            rel_key = (safe_source, safe_target, "inherits")
            if rel_key not in seen_rels:
                lines.append(f'    {safe_source} -> {safe_target} [style=dashed, label="extends"];')
                seen_rels.add(rel_key)
        else:
            rel_key = (safe_source, safe_target, label)
            if rel_key not in seen_rels:
                arrowhead = "crow" if "{" in rel_type else "normal"
                lbl = f', label="{label}"' if label else ""
                lines.append(f"    {safe_source} -> {safe_target} [arrowhead={arrowhead}{lbl}];")
                seen_rels.add(rel_key)

    lines.append("}")
    return "\n".join(lines)


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

        result = generate_dot_erd(all_schemas, no_properties, excluded_types, max_properties)

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

            basic_result = generate_dot_erd(basic_schemas, no_properties, set(), max_properties)

            Path(basic_types_output).write_text(basic_result)
            click.echo(f"Wrote {len(basic_result)} bytes to {basic_types_output} (basic types ERD)")
