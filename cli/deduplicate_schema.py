import copy
import hashlib
import json
from collections import defaultdict
from pathlib import Path

import click
import requests

# Timeout for HTTP requests in seconds.
REQUEST_TIMEOUT = 30
# Minimum occurrences to extract a schema into $defs.
MIN_OCCURRENCES = 2
# Minimum members in a family to consider grouping.
MIN_FAMILY_MEMBERS = 2
# Minimum identical properties to extract a base schema.
MIN_IDENTICAL_PROPS = 3
# Higher threshold for cross-family procedure base extraction.
MIN_PROCEDURE_BASE_PROPS = 10

PROCEDURE_TYPES = {"Subsoil", "Dgf", "RailwayCargo", "Timber", "Renewables"}
# Dutch/English property patterns (procedure-level only).
DUTCH_PROCEDURE_PROPS = {"dutchStep", "bids"}
ENGLISH_PROCEDURE_PROPS = {"minNumberOfQualifiedBids", "bids"}
ALL_MIXIN_PROPS = DUTCH_PROCEDURE_PROPS | ENGLISH_PROCEDURE_PROPS
# Bids property templates for each mixin type.
DUTCH_BIDS = {
    "type": "array",
    "x-format": "list-object",
    "items": {"$ref": "#/$defs/embed.dgf-dutch.Bid"},
    "x-legalNameEn": "Bid",
}
ENGLISH_BIDS = {
    "type": "array",
    "x-format": "list-object",
    "items": {"$ref": "#/$defs/embed.dgf-english.Bid"},
    "x-legalNameEn": "Bid",
}
# Name normalization mapping (treat these as equivalent when naming).
NAME_ALIASES = {"SellingEntity": "Organization"}
# Properties whose enum values can be collapsed (extracted to x-*-enum).
COLLAPSIBLE_ENUM_PROPS = {"documentOf", "documentType"}
# Properties to ignore entirely during deduplication (allows schemas to collapse).
COLLAPSIBLE_PROPS = {"valueAddedTaxIncluded"}
# Minimum ratio of identical properties to consider schemas for allOf inheritance.
ALLOF_SIMILARITY_THRESHOLD = 0.7
VALIDATION_AND_METADATA = {
    # https://json-schema.org/draft/2020-12/draft-bhutton-json-schema-validation-00#rfc.section.9
    "title",
    "description",
    "default",
    "deprecated",
    "readOnly",
    "example",
    # https://json-schema.org/draft/2020-12/draft-bhutton-json-schema-validation-00#rfc.section.6
    # Any
    "const",
    # Numeric
    "multipleOf",
    "maximum",
    "exclusiveMaximum",
    "minimum",
    "exclusiveMinimum",
    # String
    "maxLength",
    "minLength",
    "pattern",
    # Array
    "maxItems",
    "minItems",
    "uniqueItems",
    "maxContains",
    "minContains",
    # Object
    "minProperties",
    "maxProperties",
    "dependentRequired",
}
# Omit from hash.
DEDUPE_EXCLUDE = VALIDATION_AND_METADATA | {"enum", "required"}
# x-* keys are otherwise excluded from hash.
DEDUPE_INCLUDE = set()
# Omit from output.
OUTPUT_EXCLUDE = VALIDATION_AND_METADATA
# x-* keys are otherwise excluded from output.
OUTPUT_INCLUDE = {"x-format", "x-legalNameEn"} | {f"x-{p}-enum" for p in COLLAPSIBLE_ENUM_PROPS}


def normalize_schema(obj, *, collapse_enums=False):
    """Recursively strip metadata fields for structural comparison (hashing)."""
    if isinstance(obj, dict):
        result = {}
        for k, v in obj.items():
            if k in DEDUPE_EXCLUDE:
                continue
            if k.startswith("x-") and k not in DEDUPE_INCLUDE:
                continue
            # When collapsing, strip enum from collapsible properties and remove collapsible props
            if collapse_enums and k == "properties":
                value = _strip_collapsible_enums(v)
                value = {pk: pv for pk, pv in value.items() if pk not in COLLAPSIBLE_PROPS}
            else:
                value = v
            result[k] = normalize_schema(value, collapse_enums=collapse_enums)
        return result
    if isinstance(obj, list):
        return [normalize_schema(item, collapse_enums=collapse_enums) for item in obj]
    return obj


def _strip_collapsible_enums(properties):
    """Strip enum from collapsible properties (documentOf, documentType)."""

    def strip_enum(prop_name, prop_value):
        if prop_name in COLLAPSIBLE_ENUM_PROPS and isinstance(prop_value, dict) and "enum" in prop_value:
            return {k: v for k, v in prop_value.items() if k != "enum"}
        return prop_value

    return {k: strip_enum(k, v) for k, v in properties.items()}


def extract_collapsible_enums(schema):
    """Extract enum values from collapsible properties."""
    enums = {}
    props = schema.get("properties", {})
    for prop_name in COLLAPSIBLE_ENUM_PROPS:
        if prop_name in props and isinstance(props[prop_name], dict):
            enum_val = props[prop_name].get("enum")
            if enum_val:
                enums[f"x-{prop_name}-enum"] = enum_val
    return enums


def _remove_collapsible_enums(schema):
    """Remove enum from collapsible properties in a schema (deep copy)."""
    schema = copy.deepcopy(schema)
    props = schema.get("properties", {})
    for prop_name in COLLAPSIBLE_ENUM_PROPS:
        if prop_name in props and isinstance(props[prop_name], dict):
            props[prop_name].pop("enum", None)
    return schema


def strip_metadata(obj):
    """Recursively strip metadata fields for output, keeping display labels."""
    if isinstance(obj, dict):
        return {
            k: strip_metadata(v)
            for k, v in obj.items()
            if k not in OUTPUT_EXCLUDE and (not k.startswith("x-") or k in OUTPUT_INCLUDE)
        }
    if isinstance(obj, list):
        return [strip_metadata(v) for v in obj]
    return obj


def strip_private_properties(obj):
    """Recursively remove properties that start with underscore."""
    if isinstance(obj, dict):
        result = {}
        for k, v in obj.items():
            if k == "properties" and isinstance(v, dict):
                # Filter out underscore-prefixed properties
                value = {pk: pv for pk, pv in v.items() if not pk.startswith("_")}
            else:
                value = v
            result[k] = strip_private_properties(value)
        return result
    if isinstance(obj, list):
        return [strip_private_properties(item) for item in obj]
    return obj


def collapse_redundant_unions(obj):
    """Collapse anyOf/oneOf where all branches resolve to the same $ref."""
    if isinstance(obj, dict):
        result = {}
        for k, v in obj.items():
            if k in ("anyOf", "oneOf") and isinstance(v, (list, dict)):
                # Handle both list and dict forms of anyOf/oneOf
                items = list(v.values()) if isinstance(v, dict) else v
                # Check if all items are identical $refs
                refs = set()
                all_refs = True
                for item in items:
                    if isinstance(item, dict) and "$ref" in item and len(item) == 1:
                        refs.add(item["$ref"])
                    else:
                        all_refs = False
                        break
                # If all items are the same $ref, collapse to single ref
                if all_refs and len(refs) == 1:
                    result["$ref"] = refs.pop()
                    continue
            result[k] = collapse_redundant_unions(v)
        return result
    if isinstance(obj, list):
        return [collapse_redundant_unions(v) for v in obj]
    return obj


def deduplicate_identical_schemas(defs, schemas):
    """Find schemas that are now identical and deduplicate them."""

    def hash_for_dedup(obj):
        """Hash schema ignoring metadata for deduplication."""
        normalized = normalize_schema(obj, collapse_enums=True)
        return hashlib.md5(json.dumps(normalized, sort_keys=True).encode(), usedforsecurity=False).hexdigest()

    # Hash all defs
    hash_to_names = {}
    for name, schema in defs.items():
        h = hash_for_dedup(schema)
        hash_to_names.setdefault(h, []).append(name)

    # Find duplicates and create mapping
    canonical = {}  # name -> canonical name
    for names in hash_to_names.values():
        if len(names) > 1:
            # Keep the shortest/simplest name as canonical
            names_sorted = sorted(names, key=lambda n: (len(n), n))
            canon = names_sorted[0]
            for name in names_sorted[1:]:
                canonical[name] = canon

    if not canonical:
        return defs, schemas

    # Replace references to duplicates with canonical
    def replace_refs(obj):
        if isinstance(obj, dict):
            if "$ref" in obj:
                ref_name = obj["$ref"].split("/")[-1]
                if ref_name in canonical:
                    return {**obj, "$ref": f"#/$defs/{canonical[ref_name]}"}
            return {k: replace_refs(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [replace_refs(v) for v in obj]
        return obj

    # Remove duplicate defs and update references
    new_defs = {name: replace_refs(schema) for name, schema in defs.items() if name not in canonical}
    new_schemas = {name: replace_refs(schema) for name, schema in schemas.items()}

    return new_defs, new_schemas


def apply_allof_inheritance(schemas, defs):
    """
    Find similar schemas and extract common properties using allOf.

    Returns updated (schemas, defs) with base schemas extracted.
    """
    # Find schemas with properties that could share a base
    candidates = {
        name: schema
        for name, schema in {**schemas, **defs}.items()
        if isinstance(schema, dict) and "properties" in schema
    }

    def get_family(name):
        """Extract family prefix from schema name (e.g., 'subsoil' from 'subsoil-english.Foo')."""
        base = name.split(".")[0] if "." in name else name
        return base.split("-")[0] if "-" in base else base

    def get_semantic_type(name, schema):
        """Determine semantic type of schema to prevent mixing Bids with Awards, etc."""
        props = set(schema.get("properties", {}).keys())
        name_lower = name.lower()

        # Check name first - it's more reliable than properties
        # (e.g., Contract schemas have 'buyers' property but aren't Awards)
        if "contract" in name_lower:
            return "contract"
        if "cancellation" in name_lower:
            return "cancellation"

        # Then check properties for Bid vs Award distinction
        if "bidders" in props:
            return "bid"
        if "buyers" in props:
            return "award"

        # Fall back to name hints
        if "bid" in name_lower:
            return "bid"
        if "award" in name_lower:
            return "award"

        return "other"

    def are_semantically_compatible(name1, schema1, name2, schema2):
        """Check if two schemas are semantically compatible for grouping."""
        type1 = get_semantic_type(name1, schema1)
        type2 = get_semantic_type(name2, schema2)
        # Only group schemas of the same semantic type, or if both are "other"
        return type1 == type2

    def count_identical_props(s1, s2):
        """Count identical properties between two schemas (using normalized comparison)."""
        p1, p2 = s1.get("properties", {}), s2.get("properties", {})
        common_keys = set(p1.keys()) & set(p2.keys())
        return sum(
            1
            for k in common_keys
            if json.dumps(normalize_schema(p1[k]), sort_keys=True)
            == json.dumps(normalize_schema(p2[k]), sort_keys=True)
        )

    # Group by family first, then by similarity
    families = {}
    for name in candidates:
        family = get_family(name)
        families.setdefault(family, []).append(name)

    groups = []
    used = set()

    # First pass: group within families (same prefix like "subsoil-*")
    for members in families.values():
        if len(members) < MIN_FAMILY_MEMBERS:
            continue

        for name1 in members:
            if name1 in used:
                continue
            s1 = candidates[name1]
            p1 = s1.get("properties", {})
            group = [name1]

            for name2 in members:
                if name2 == name1 or name2 in used:
                    continue
                s2 = candidates[name2]
                p2 = s2.get("properties", {})

                # Check semantic compatibility before structural similarity
                if not are_semantically_compatible(name1, s1, name2, s2):
                    continue

                identical = count_identical_props(s1, s2)
                min_props = min(len(p1), len(p2))
                if min_props > 0 and identical / min_props >= ALLOF_SIMILARITY_THRESHOLD:
                    group.append(name2)

            if len(group) > 1:
                groups.append(group)
                used.update(group)

    # Second pass: group remaining schemas across families
    remaining = [n for n in candidates if n not in used]
    for name1 in remaining:
        if name1 in used:
            continue
        s1 = candidates[name1]
        p1 = s1.get("properties", {})
        group = [name1]

        for name2 in remaining:
            if name2 == name1 or name2 in used:
                continue
            s2 = candidates[name2]
            p2 = s2.get("properties", {})

            # Check semantic compatibility before structural similarity
            if not are_semantically_compatible(name1, s1, name2, s2):
                continue

            identical = count_identical_props(s1, s2)
            min_props = min(len(p1), len(p2))
            if min_props > 0 and identical / min_props >= ALLOF_SIMILARITY_THRESHOLD:
                group.append(name2)

        if len(group) > 1:
            groups.append(group)
            used.update(group)

    def extract_base(group, candidates_dict, target_defs, target_schemas, base_name_hint=None):
        """Extract common properties from a group into a base schema."""
        group_schemas = [(n, candidates_dict[n]) for n in group]

        # Find identical properties across all schemas in group
        all_props = [set(s.get("properties", {}).keys()) for _, s in group_schemas]
        common_keys = set.intersection(*all_props) if all_props else set()

        # Filter to only identical values (using normalized comparison)
        first_props = group_schemas[0][1].get("properties", {})
        identical_props = {}
        for key in common_keys:
            first_val = json.dumps(normalize_schema(first_props[key]), sort_keys=True)
            if all(
                json.dumps(normalize_schema(s.get("properties", {}).get(key)), sort_keys=True) == first_val
                for _, s in group_schemas
            ):
                identical_props[key] = first_props[key]

        if len(identical_props) < MIN_IDENTICAL_PROPS:  # Not worth extracting fewer than 3 properties
            return None

        # Create base schema name (prefix with "ocp." for generated schemas)
        if base_name_hint:
            base_name = f"ocp.{base_name_hint}"
        else:
            names = [n.split(".")[-1] for n in group]
            common_prefix = ""
            for chars in zip(*names, strict=False):
                if len(set(chars)) == 1:
                    common_prefix += chars[0]
                else:
                    break
            if common_prefix:
                base_name = f"ocp.{common_prefix}Base"
            else:
                # Use semantic type for naming
                first_schema = group_schemas[0][1]
                sem_type = get_semantic_type(group[0], first_schema)
                type_to_name = {
                    "bid": "BidBase",
                    "award": "AwardBase",
                    "contract": "ContractBase",
                    "cancellation": "CancellationBase",
                }
                if sem_type in type_to_name:
                    base_name = f"ocp.{type_to_name[sem_type]}"
                else:
                    base_name = f"ocp.SharedBase_{len(target_defs)}"

        # Create base schema
        base_schema = {
            "type": "object",
            "properties": identical_props,
        }
        target_defs[base_name] = base_schema

        # Update each schema to use allOf
        for name, schema in group_schemas:
            remaining_props = {k: v for k, v in schema.get("properties", {}).items() if k not in identical_props}

            new_schema = {
                "allOf": [
                    {"$ref": f"#/$defs/{base_name}"},
                ]
            }

            if remaining_props:
                new_schema["allOf"].append(
                    {
                        "type": "object",
                        "properties": remaining_props,
                    }
                )

            # Preserve other schema keys (like required)
            new_schema.update({k: v for k, v in schema.items() if k not in ("type", "properties")})

            if name in target_schemas:
                target_schemas[name] = new_schema
            else:
                target_defs[name] = new_schema

        return base_name

    # For each group, extract common properties
    new_defs = dict(defs)
    new_schemas = dict(schemas)

    for group in groups:
        extract_base(group, candidates, new_defs, new_schemas)

    # Third pass: look for common properties across family bases (e.g., RailwayCargoBase, SubsoilBase, DgfBase)
    # Only consider the main procedure bases, not component bases
    procedure_bases = [
        name
        for name in new_defs
        if name.endswith("Base") and "properties" in new_defs[name] and any(x in name for x in PROCEDURE_TYPES)
    ]
    if len(procedure_bases) >= MIN_FAMILY_MEMBERS:
        # Check how many identical properties they share
        base_props = {name: new_defs[name].get("properties", {}) for name in procedure_bases}
        all_keys = [set(p.keys()) for p in base_props.values()]
        common_keys = set.intersection(*all_keys) if all_keys else set()

        # Count identical (using normalized comparison)
        if common_keys:
            first_base = procedure_bases[0]
            first_props = base_props[first_base]
            identical_keys = []
            for key in common_keys:
                first_val = json.dumps(normalize_schema(first_props[key]), sort_keys=True)
                if all(
                    json.dumps(normalize_schema(base_props[name][key]), sort_keys=True) == first_val
                    for name in procedure_bases
                ):
                    identical_keys.append(key)

            # If enough identical properties, extract ProcedureBase
            if len(identical_keys) >= MIN_PROCEDURE_BASE_PROPS:
                extract_base(procedure_bases, new_defs, new_defs, new_schemas, base_name_hint="ProcedureBase")

                # Fourth pass: make standalone procedure schemas inherit from ocp.ProcedureBase
                proc_base_props = new_defs["ocp.ProcedureBase"].get("properties", {})
                extra_counter = 0
                renames = {}  # old_name -> new_name for SharedBase_N schemas
                for name in list(new_schemas.keys()) + list(new_defs.keys()):
                    # Skip if already using allOf or is a Base schema
                    schema = new_schemas.get(name) or new_defs.get(name)
                    if not isinstance(schema, dict) or "allOf" in schema or name.endswith("Base"):
                        continue
                    if "properties" not in schema:
                        continue

                    # Check if this looks like a procedure schema (has most ProcedureBase props)
                    schema_props = schema.get("properties", {})
                    matching = sum(
                        1
                        for k in proc_base_props
                        if k in schema_props
                        and json.dumps(normalize_schema(schema_props[k]), sort_keys=True)
                        == json.dumps(normalize_schema(proc_base_props[k]), sort_keys=True)
                    )

                    # If it has most of ProcedureBase's properties, make it inherit
                    if matching >= len(proc_base_props) * 0.8:
                        remaining_props = {k: v for k, v in schema_props.items() if k not in proc_base_props}
                        new_schema = {"allOf": [{"$ref": "#/$defs/ocp.ProcedureBase"}]}
                        if remaining_props:
                            new_schema["allOf"].append({"type": "object", "properties": remaining_props})
                        new_schema.update({k: v for k, v in schema.items() if k not in ("type", "properties")})

                        # Rename SharedBase_N to ProcedureBaseExtra
                        final_name = name
                        if "SharedBase_" in name:
                            extra_suffix = "" if extra_counter == 0 else str(extra_counter + 1)
                            final_name = f"ocp.ProcedureBaseExtra{extra_suffix}"
                            extra_counter += 1
                            renames[name] = final_name

                        if name in new_schemas:
                            new_schemas[final_name] = new_schema
                            if final_name != name:
                                del new_schemas[name]
                        else:
                            new_defs[final_name] = new_schema
                            if final_name != name:
                                del new_defs[name]

                # Update references to renamed schemas
                if renames:

                    def update_refs(obj):
                        if isinstance(obj, dict):
                            if "$ref" in obj:
                                ref_name = obj["$ref"].split("/")[-1]
                                if ref_name in renames:
                                    return {**obj, "$ref": f"#/$defs/{renames[ref_name]}"}
                            return {k: update_refs(v) for k, v in obj.items()}
                        if isinstance(obj, list):
                            return [update_refs(v) for v in obj]
                        return obj

                    new_schemas = {k: update_refs(v) for k, v in new_schemas.items()}
                    new_defs = {k: update_refs(v) for k, v in new_defs.items()}

    return new_schemas, new_defs


def extract_common_from_siblings(schemas, defs):
    """
    Extract common properties from schemas that share the same base.

    For example, if multiple Award schemas extend ocp.AwardBase and all have
    'bidId' and 'terminationReason', extract those into ocp.AwardBaseExtra.

    Returns updated (schemas, defs).
    """

    def get_base_ref(schema):
        """Get the first $ref from an allOf schema."""
        if "allOf" in schema:
            for item in schema["allOf"]:
                if "$ref" in item:
                    return item["$ref"].split("/")[-1]
        return None

    def get_own_props(schema):
        """Get properties defined directly on this schema (not inherited)."""
        if "allOf" in schema:
            for item in schema["allOf"]:
                if "properties" in item:
                    return item.get("properties", {})
        return {}

    def get_schema_type_suffix(name):
        """Extract type suffix from schema name (e.g., 'Award' from 'embed.dgf.Award')."""
        # Remove 'Base' suffix if present
        suffix = name.split(".")[-1]
        return suffix.removesuffix("Base")

    # Group schemas by their base AND type suffix
    # This prevents Contract from being grouped with Award even if they share a base
    all_schemas = {**schemas, **defs}
    by_base_and_type = {}
    for name, schema in all_schemas.items():
        if not isinstance(schema, dict):
            continue
        base = get_base_ref(schema)
        if base and base.startswith("ocp.") and base.endswith("Base"):
            type_suffix = get_schema_type_suffix(name)
            key = (base, type_suffix)
            by_base_and_type.setdefault(key, []).append(name)

    # For each (base, type) group with multiple children, look for common properties
    for (base_name, type_suffix), children in by_base_and_type.items():
        if len(children) < MIN_FAMILY_MEMBERS:
            continue

        # Collect properties from each child
        child_props = {}
        for child_name in children:
            child_schema = all_schemas[child_name]
            props = get_own_props(child_schema)
            child_props[child_name] = props

        # Find properties common to all children
        all_prop_names = [set(p.keys()) for p in child_props.values()]
        common_names = set.intersection(*all_prop_names) if all_prop_names else set()

        # Filter to only identical values (using normalized comparison)
        first_child = children[0]
        first_props = child_props[first_child]
        identical_props = {}
        for prop_name in common_names:
            first_val = json.dumps(normalize_schema(first_props[prop_name]), sort_keys=True)
            if all(
                json.dumps(normalize_schema(child_props[c].get(prop_name)), sort_keys=True) == first_val
                for c in children
            ):
                identical_props[prop_name] = first_props[prop_name]

        # Need at least some common properties to extract
        if len(identical_props) < MIN_FAMILY_MEMBERS:
            continue

        # Create intermediate base
        # Name it based on the type (e.g., Award schemas -> ocp.AwardBaseExtra)
        # Use the type suffix to handle cases where different types extend the same base
        extra_name = f"ocp.{type_suffix}BaseExtra"
        if extra_name in defs:
            # Already exists, skip
            continue

        defs[extra_name] = {
            "allOf": [
                {"$ref": f"#/$defs/{base_name}"},
                {"type": "object", "properties": identical_props},
            ]
        }

        # Update children to extend the new intermediate base
        for child_name in children:
            child_schema = all_schemas[child_name]
            own_props = child_props[child_name]
            remaining_props = {k: v for k, v in own_props.items() if k not in identical_props}

            # Rebuild the schema
            new_allof = [{"$ref": f"#/$defs/{extra_name}"}]
            if remaining_props:
                new_allof.append({"type": "object", "properties": remaining_props})

            # Copy other allOf items (like mixins) except the old base and properties
            for item in child_schema.get("allOf", []):
                if "$ref" in item:
                    ref = item["$ref"].split("/")[-1]
                    if ref != base_name:
                        new_allof.append(item)

            new_schema = {"allOf": new_allof}
            # Preserve required
            if "required" in child_schema:
                new_schema["required"] = child_schema["required"]

            if child_name in schemas:
                schemas[child_name] = new_schema
            else:
                defs[child_name] = new_schema

    return schemas, defs


def extract_dutch_english_mixins(schemas, defs):
    """
    Extract Dutch/English mixins from procedure and bid schemas.

    Creates ocp.DutchMixin and ocp.EnglishMixin with the unique properties,
    and for schemas sharing the same base, extracts common properties.

    Returns updated (schemas, defs).
    """

    def get_base_ref(schema):
        """Get the base $ref from an allOf schema."""
        if "allOf" in schema:
            for item in schema["allOf"]:
                if "$ref" in item:
                    return item["$ref"].split("/")[-1]
        return None

    def get_own_props(schema):
        """Get properties defined directly on this schema (not inherited)."""
        if "allOf" in schema:
            for item in schema["allOf"]:
                if "properties" in item:
                    return item.get("properties", {})
        return schema.get("properties", {})

    # First pass: collect mixin properties (excluding bids, which we handle specially)
    dutch_procedure_props = {}
    english_procedure_props = {}

    all_schemas = {**schemas, **defs}
    for schema in all_schemas.values():
        own_props = get_own_props(schema)
        for prop_name, prop_value in own_props.items():
            if prop_name == "bids":
                continue  # Handle bids specially below
            if prop_name in DUTCH_PROCEDURE_PROPS:
                dutch_procedure_props[prop_name] = prop_value
            elif prop_name in ENGLISH_PROCEDURE_PROPS:
                english_procedure_props[prop_name] = prop_value

    # Add bids to mixins with the correct bid type
    dutch_procedure_props["bids"] = DUTCH_BIDS
    english_procedure_props["bids"] = ENGLISH_BIDS

    # Create mixins
    if dutch_procedure_props:
        defs["ocp.DutchMixin"] = {"type": "object", "properties": dutch_procedure_props}
    if english_procedure_props:
        defs["ocp.EnglishMixin"] = {"type": "object", "properties": english_procedure_props}

    # Second pass: find Dutch/English pairs by looking for schemas with same base
    by_base = {}
    for name, schema in all_schemas.items():
        base = get_base_ref(schema)
        if base:
            by_base.setdefault(base, []).append(name)

    for base_name, derived_names in by_base.items():
        if len(derived_names) < MIN_FAMILY_MEMBERS:
            continue

        # Separate Dutch and English variants
        dutch_schemas = [(n, all_schemas[n]) for n in derived_names if "-dutch" in n.lower() or "dutch" in n.lower()]
        english_schemas = [
            (n, all_schemas[n]) for n in derived_names if "-english" in n.lower() or "english" in n.lower()
        ]

        if not dutch_schemas or not english_schemas:
            continue

        # Get properties from first of each type
        _dutch_name, dutch_schema = dutch_schemas[0]
        _english_name, english_schema = english_schemas[0]

        dutch_props = get_own_props(dutch_schema)
        english_props = get_own_props(english_schema)

        # Find common properties (identical in both, excluding mixin props)
        common_keys = set(dutch_props.keys()) & set(english_props.keys())
        common_props = {}
        for key in common_keys:
            if key in ALL_MIXIN_PROPS:
                continue
            dutch_val = json.dumps(normalize_schema(dutch_props[key]), sort_keys=True)
            english_val = json.dumps(normalize_schema(english_props[key]), sort_keys=True)
            if dutch_val == english_val:
                common_props[key] = dutch_props[key]

        # Find Dutch-only and English-only properties (excluding mixin props)
        dutch_only = {k: v for k, v in dutch_props.items() if k not in common_props and k not in ALL_MIXIN_PROPS}
        english_only = {k: v for k, v in english_props.items() if k not in common_props and k not in ALL_MIXIN_PROPS}

        # Determine what type of schema this is from the derived schema names (e.g., "Bid" from "dgf-dutch.Bid")
        type_names = [n.split(".")[-1] for n in derived_names if "-dutch" in n or "-english" in n]
        # Find common suffix (e.g., "Bid" from ["Bid", "Bid"])
        if type_names and all(t == type_names[0] for t in type_names):
            schema_type = type_names[0]
        else:
            schema_type = base_name.split(".")[-1] if "." in base_name else base_name

        # Create intermediate base with common properties if there are any
        intermediate_name = None
        if common_props:
            intermediate_name = f"ocp.{schema_type}Base"
            if intermediate_name not in defs:
                defs[intermediate_name] = {
                    "allOf": [
                        {"$ref": f"#/$defs/{base_name}"},
                        {"type": "object", "properties": common_props},
                    ]
                }

        # Update all Dutch schemas
        for name, schema in dutch_schemas:
            own_props = get_own_props(schema)
            has_dutch_procedure_props = any(p in own_props for p in DUTCH_PROCEDURE_PROPS)

            base_ref = intermediate_name or base_name
            new_allof = [{"$ref": f"#/$defs/{base_ref}"}]
            if has_dutch_procedure_props and dutch_procedure_props:
                new_allof.append({"$ref": "#/$defs/ocp.DutchMixin"})
            if dutch_only:
                new_allof.append({"type": "object", "properties": dutch_only})
            new_schema = {"allOf": new_allof}
            # Preserve required
            if "required" in schema:
                new_schema["required"] = schema["required"]
            if name in schemas:
                schemas[name] = new_schema
            else:
                defs[name] = new_schema

        # Update all English schemas
        for name, schema in english_schemas:
            own_props = get_own_props(schema)
            has_english_procedure_props = any(p in own_props for p in ENGLISH_PROCEDURE_PROPS)

            base_ref = intermediate_name or base_name
            new_allof = [{"$ref": f"#/$defs/{base_ref}"}]
            if has_english_procedure_props and english_procedure_props:
                new_allof.append({"$ref": "#/$defs/ocp.EnglishMixin"})
            if english_only:
                new_allof.append({"type": "object", "properties": english_only})
            new_schema = {"allOf": new_allof}
            # Preserve required
            if "required" in schema:
                new_schema["required"] = schema["required"]
            if name in schemas:
                schemas[name] = new_schema
            else:
                defs[name] = new_schema

    return schemas, defs


def extract_inline_with_allof(schemas, defs):
    """
    Find remaining inline schemas and extract them using allOf if similar to existing defs.

    Returns updated (schemas, defs).
    """

    def count_identical_props(s1, s2):
        """Count identical properties between two schemas (using normalized comparison)."""
        p1, p2 = s1.get("properties", {}), s2.get("properties", {})
        common_keys = set(p1.keys()) & set(p2.keys())
        return sum(
            1
            for k in common_keys
            if json.dumps(normalize_schema(p1[k]), sort_keys=True)
            == json.dumps(normalize_schema(p2[k]), sort_keys=True)
        )

    def find_best_base(inline_schema, existing_defs):
        """Find the best matching base schema for an inline schema."""
        inline_props = inline_schema.get("properties", {})
        if len(inline_props) < MIN_IDENTICAL_PROPS:
            return None, 0

        best_match = None
        best_identical = 0

        for def_name, def_schema in existing_defs.items():
            # Skip schemas that use allOf (they're already derived)
            if "allOf" in def_schema:
                continue

            def_props = def_schema.get("properties", {})
            if not def_props:
                continue

            identical = count_identical_props(inline_schema, def_schema)
            # Score by fraction of base schema properties that match
            if len(def_props) > 0:
                pct = identical / len(def_props)
                # Require at least 70% match and minimum matching properties
                # Among qualifying bases, prefer the one with most matching properties
                meets_threshold = pct >= ALLOF_SIMILARITY_THRESHOLD and identical >= MIN_IDENTICAL_PROPS
                if meets_threshold and identical > best_identical:
                    best_identical = identical
                    best_match = def_name

        return best_match, best_identical

    def extract_from_object(obj, path, new_defs, replacements):
        """Recursively find inline schemas and create allOf-based replacements."""
        if isinstance(obj, dict):
            # Check if this is an inline schema with properties
            if "properties" in obj and "$ref" not in obj and "allOf" not in obj:
                base_name, _score = find_best_base(obj, defs)
                if base_name:
                    # Create a new schema using allOf
                    base_props = defs[base_name].get("properties", {})
                    remaining_props = {k: v for k, v in obj.get("properties", {}).items() if k not in base_props}

                    new_schema = {"allOf": [{"$ref": f"#/$defs/{base_name}"}]}
                    if remaining_props:
                        new_schema["allOf"].append({"type": "object", "properties": remaining_props})

                    # Preserve required
                    if "required" in obj:
                        new_schema["required"] = obj["required"]

                    # Generate a name based on procedure family + base schema type
                    path_parts = [p for p in path if p and not p.startswith("[")]
                    # Get the procedure family prefix
                    if path_parts:
                        proc_name = path_parts[0]
                        prefix = proc_name.split(".")[0] if "." in proc_name else proc_name.rsplit("-", 1)[0]
                    else:
                        prefix = "ocp"
                    # Get the type name from the base schema
                    base_type = base_name.split(".")[-1]
                    new_name = f"embed.{prefix}.{base_type}"

                    # Add to new defs and mark for replacement
                    if new_name not in new_defs and new_name not in defs:
                        new_defs[new_name] = new_schema
                        replacements[id(obj)] = {"$ref": f"#/$defs/{new_name}"}

            # Recurse into properties
            for k, v in obj.items():
                extract_from_object(v, [*path, k], new_defs, replacements)

        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                extract_from_object(v, [*path, f"[{i}]"], new_defs, replacements)

    def apply_replacements(obj, replacements):
        """Apply the collected replacements."""
        if id(obj) in replacements:
            return replacements[id(obj)]
        if isinstance(obj, dict):
            return {k: apply_replacements(v, replacements) for k, v in obj.items()}
        if isinstance(obj, list):
            return [apply_replacements(v, replacements) for v in obj]
        return obj

    # Find all inline schemas and their best base matches
    new_defs = {}
    replacements = {}

    for schema_name, schema in schemas.items():
        extract_from_object(schema, [schema_name], new_defs, replacements)

    # Apply replacements
    if replacements:
        schemas = {name: apply_replacements(schema, replacements) for name, schema in schemas.items()}

    # Merge new defs
    defs = {**defs, **new_defs}

    return schemas, defs


def extract_refs(obj, refs):
    """Recursively extract all $ref schema names from an object."""
    if isinstance(obj, dict):
        if "$ref" in obj:
            refs.add(obj["$ref"].split("/")[-1])
        for v in obj.values():
            extract_refs(v, refs)
    elif isinstance(obj, list):
        for v in obj:
            extract_refs(v, refs)


def filter_get_schemas(spec, schemas):
    """Filter schemas to only those referenced by GET operations."""
    paths = spec.get("paths", {})
    get_refs = set()

    for methods in paths.values():
        if "get" in methods:
            for resp_obj in methods["get"].get("responses", {}).values():
                extract_refs(resp_obj, get_refs)

    return {name: schema for name, schema in schemas.items() if name in get_refs}


def register_command(cli):
    @cli.command()
    @click.argument("url")
    @click.option("-o", "--output", type=click.Path(), help="Output file (default: stdout)")
    @click.option("--get-only", is_flag=True, help="Only include schemas used by GET operations")
    @click.option(
        "--ignore-metadata",
        is_flag=True,
        help="Ignore metadata fields (readOnly, description, title, example, default, x-*) when deduplicating",
    )
    @click.option(
        "--collapse-enums",
        is_flag=True,
        help="Collapse schemas that differ only in documentOf/documentType enums",
    )
    @click.option(
        "--use-allof",
        is_flag=True,
        help="Use allOf inheritance for similar schemas sharing common properties",
    )
    @click.option(
        "--extract-mixins",
        is_flag=True,
        help="Extract Dutch/English-specific properties into shared mixins (requires --use-allof)",
    )
    def deduplicate_schema(url, output, get_only, ignore_metadata, collapse_enums, use_allof, extract_mixins):
        """
        Fetch an OpenAPI spec and output deduplicated JSON Schema.

        Extracts repeated inline schemas into $defs and replaces them with $ref.
        """
        # 1. Fetch the spec
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        spec = response.json()

        # 2. Get the schemas section
        schemas = spec.get("components", {}).get("schemas", spec.get("definitions", {}))

        # 2a. Filter to GET-only schemas if requested
        if get_only:
            schemas = filter_get_schemas(spec, schemas)

        # 2b. Remove private properties (underscore-prefixed)
        schemas = {name: strip_private_properties(schema) for name, schema in schemas.items()}

        # 2c. Collapse anyOf/oneOf early (before deduplication) so identical branches are unified
        schemas = {name: collapse_redundant_unions(schema) for name, schema in schemas.items()}

        # 3. First pass: find all inline objects and count occurrences by hash
        def hash_schema(obj):
            to_hash = normalize_schema(obj, collapse_enums=collapse_enums) if ignore_metadata else obj
            return hashlib.md5(json.dumps(to_hash, sort_keys=True).encode(), usedforsecurity=False).hexdigest()

        occurrences = defaultdict(lambda: {"count": 0, "schema": None, "title": None, "is_embedded": False})

        def count_inline(obj, *, in_array=False):
            if isinstance(obj, dict):
                # Match objects with properties (type: object is often implicit)
                if "properties" in obj:
                    h = hash_schema(obj)
                    occurrences[h]["count"] += 1
                    # Track if found inside array items
                    if in_array:
                        occurrences[h]["is_embedded"] = True
                    # Prefer schema with more properties (for collapsible prop merging)
                    existing = occurrences[h]["schema"]
                    if existing is None or len(obj.get("properties", {})) > len(existing.get("properties", {})):
                        occurrences[h]["schema"] = obj
                        occurrences[h]["title"] = obj.get("title")
                for k, v in obj.items():
                    # Track if we're entering array items
                    count_inline(v, in_array=(k == "items"))
            elif isinstance(obj, list):
                for v in obj:
                    count_inline(v, in_array=in_array)

        for schema in schemas.values():
            count_inline(schema)

        # 4. Build $defs from schemas appearing 2+ times
        defs = {}
        hash_to_name = {}
        name_counts = defaultdict(int)

        for h, data in occurrences.items():
            if data["count"] >= MIN_OCCURRENCES:
                base_name = data["title"] or f"Schema_{h[:8]}"
                # Apply name aliases (e.g., SellingEntity -> Organization)
                for alias, canonical in NAME_ALIASES.items():
                    if base_name.endswith(alias):
                        base_name = base_name[: -len(alias)] + canonical
                # Add embed. prefix for schemas found inside array items
                if data["is_embedded"]:
                    base_name = f"embed.{base_name}"
                name_counts[base_name] += 1
                name = base_name if name_counts[base_name] == 1 else f"{base_name}_{name_counts[base_name]}"
                schema = data["schema"]
                # When collapsing enums, store schema without the collapsible enums
                if collapse_enums:
                    schema = _remove_collapsible_enums(schema)
                defs[name] = schema
                hash_to_name[h] = name

        # 5. Second pass: replace inline schemas with $ref
        def replace_inline(obj, exclude_name=None):
            if isinstance(obj, dict):
                # Match objects with properties (type: object is often implicit)
                if "properties" in obj:
                    h = hash_schema(obj)
                    if h in hash_to_name and hash_to_name[h] != exclude_name:
                        ref = {"$ref": f"#/$defs/{hash_to_name[h]}"}
                        # When collapsing enums, preserve the enum values as x-*-enum
                        if collapse_enums:
                            ref.update(extract_collapsible_enums(obj))
                        return ref
                return {k: replace_inline(v, exclude_name) for k, v in obj.items()}
            if isinstance(obj, list):
                return [replace_inline(v, exclude_name) for v in obj]
            return obj

        result_schemas = {name: replace_inline(schema) for name, schema in schemas.items()}
        # For defs, exclude self-references but allow references to other defs
        defs = {name: replace_inline(schema, exclude_name=name) for name, schema in defs.items()}

        # 6. Strip metadata from output if requested
        if ignore_metadata:
            defs = {name: strip_metadata(schema) for name, schema in defs.items()}
            result_schemas = {name: strip_metadata(schema) for name, schema in result_schemas.items()}

        # 6a. Collapse anyOf/oneOf with identical branches
        defs = {name: collapse_redundant_unions(schema) for name, schema in defs.items()}
        result_schemas = {name: collapse_redundant_unions(schema) for name, schema in result_schemas.items()}

        # 6b. Re-deduplicate schemas that became identical after anyOf collapse
        if ignore_metadata:
            defs, result_schemas = deduplicate_identical_schemas(defs, result_schemas)

        # 6c. Apply allOf inheritance for similar schemas
        if use_allof:
            result_schemas, defs = apply_allof_inheritance(result_schemas, defs)

        # 6d. Extract remaining inline schemas using allOf if similar to existing defs
        if use_allof:
            result_schemas, defs = extract_inline_with_allof(result_schemas, defs)

        # 6e. Extract common properties from sibling schemas (same base)
        if use_allof:
            result_schemas, defs = extract_common_from_siblings(result_schemas, defs)

        # 6f. Extract Dutch/English mixins
        if extract_mixins:
            if not use_allof:
                raise click.UsageError("--extract-mixins requires --use-allof")
            result_schemas, defs = extract_dutch_english_mixins(result_schemas, defs)

        # 6g. Re-deduplicate schemas that became identical after allOf passes
        if use_allof:
            defs, result_schemas = deduplicate_identical_schemas(defs, result_schemas)

        # 7. Build output JSON Schema
        result = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": defs,
            "schemas": result_schemas,
        }

        # 8. Output
        out = json.dumps(result, indent=2)
        if output:
            Path(output).write_text(out)
            click.echo(f"Wrote {len(out)} bytes to {output}")
        else:
            click.echo(out)
