import functools

from cli.util import ref_name


def _safe_name(name):
    """Convert schema name to safe identifier for diagrams."""
    # DOT identifiers must be alphanumeric or underscores (unless quoted).
    # - is an edge operator and . is a port separator in DOT syntax.
    # Spaces would require quoting. | delimits record fields in node labels.
    return name.replace("-", "_").replace(".", "_").replace(" ", "_").replace("|", "_or_")


def _iter_union_options(union_value):
    """Iterate over anyOf/oneOf options, handling both list and dict forms."""
    if isinstance(union_value, list):
        yield from union_value
    elif isinstance(union_value, dict):
        yield from union_value.values()


def _extract_prop_relationships(name, properties):
    """Extract relationships from properties, handling anyOf/oneOf/allOf and arrays."""
    relationships = []
    for prop_name, prop_schema in properties.items():
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

        for target, is_array in refs:
            rel_type = "||--o{" if is_array else "||--o|"
            relationships.append((name, target, rel_type, prop_name))
    return relationships


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


def generate_erd(all_schemas, no_properties, excluded=None, max_properties=10):
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
