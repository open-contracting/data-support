# OCDS Transformer for TenderNed

Transform the dataset published in https://www.tenderned.nl/cms/nl/aanbesteden-in-cijfers/datasets-aanbestedingen
to OCDS, using a mapping file that maps each column from the original dataset to an OCDS field.

### How to use

- Install the requirements with `pip install -r requirements.txt`.
- Set the file name to transform in `transform.py` file, in the `FILE_NAME` variable.
- Run the script with `python transform.py`. You can also transform only one year, with `python transform.py --year 2022`, for example.
- To convert the original data to a OCDS JSON, a patched schema with extensions is generated. If you want to re-generate
the schema, you can run `python transform.py --generate-schema`.

The script will generate one JSON file per year in the "ocds" folder.
