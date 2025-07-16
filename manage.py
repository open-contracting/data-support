#!/usr/bin/env python
import json
from pathlib import Path

import click


@click.group()
def cli():
    pass


@cli.command()
def update_crowdinyml_files():
    """
    Update the files in crowdin.yml.

    Similar to sphinx-intl update-txconfig-resources.
    """
    cwd = Path.cwd()

    configfile = cwd / "crowdin.yml"
    if not configfile.exists():
        raise click.ClickException(f"crowdin.yml not found in {cwd}")

    with configfile.open() as f:
        config = json.load(f)

    builddir = cwd / "docs" / "_build" / "gettext"
    if not builddir.exists():
        raise click.ClickException(f".pot files not found in {builddir}. Run: make gettext")

    config["files"] = [
        {
            "source": f"/{pot.relative_to(cwd)}",
            "dest": f"{pot.relative_to(builddir)}",
            "translation": f"/docs/locale/%two_letters_code%/LC_MESSAGES/{str(pot.relative_to(builddir))[:-1]}",
        }
        for pot in sorted(builddir.glob("**/*.pot"))
    ]

    with configfile.open("w") as f:
        json.dump(config, f, indent=2)


if __name__ == "__main__":
    cli()
