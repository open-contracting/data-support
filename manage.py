#!/usr/bin/env python
import json
from pathlib import Path

import click


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "-p",
    "--pot-dir",
    type=click.Path(exists=True, file_okay=False, resolve_path=True, path_type=Path),
    help="The directory containing POT files, like docs/_build/gettext",
)
@click.option(
    "-d",
    "--locale-dir",
    type=click.Path(exists=True, file_okay=False, resolve_path=True, path_type=Path),
    help="The directory containing PO files, like docs/locale",
)
@click.option("--project-id", type=int, help="The Crowdin project ID")
@click.option("--ignore", multiple=True, help="Substrings of path to ignore")
def update_crowdinyml_files(pot_dir, locale_dir, project_id, ignore):
    """
    Update the files in crowdin.yml.

    Similar to sphinx-intl update-txconfig-resources.
    """
    cwd = Path.cwd()

    config = {}

    configfile = cwd / "crowdin.yml"
    if configfile.exists():
        with configfile.open() as f:
            text = f.read()
            if text:
                try:
                    config = json.loads(text)
                except json.JSONDecodeError:
                    click.secho(f"{configfile} might be YAML, overwriting", fg="red", err=True)

    if project_id:
        config["project_id"] = project_id
    config["preserve_hierarchy"] = True

    if pot_dir and locale_dir:
        if not pot_dir.exists():
            raise click.ClickException(f".pot files not found in {pot_dir}. For Sphinx, run: make gettext")

        infix = locale_dir.relative_to(cwd)
        config["files"] = [
            {
                "source": f"/{pot.relative_to(cwd)}",
                "dest": f"{pot.relative_to(pot_dir)}",
                "translation": f"/{infix}/%two_letters_code%/LC_MESSAGES/{str(pot.relative_to(pot_dir))[:-1]}",
            }
            for pot in sorted(pot_dir.glob("**/*.pot"))
            if not any(pattern in str(pot) for pattern in ignore)
        ]

    with configfile.open("w") as f:
        json.dump(config, f, indent=2)


if __name__ == "__main__":
    cli()
