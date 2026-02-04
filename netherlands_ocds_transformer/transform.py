import argparse
import datetime
import json
import os
import shutil
from urllib.request import urlopen

import json_merge_patch
import numpy as np
import ocdskit.combine
import pandas as pd
from flattentool import unflatten
from ocdsextensionregistry import ProfileBuilder

# Config and meta data settings
OCID_PREFIX = "ocds-1l04xe-"
CSV_OUTPUT_DIR = "data"
JSON_OUTPUT_DIR = "ocds"
FILE_NAME = "Dataset_TenderNed_2016_2022.xlsx"
DATA_SHEET_NAME = "Dataset 2016-2022"
OCDS_MAP_COLUMN = "OCDS path"
TENDERNED_SOURCE_COLUMN = "Veldnaam TenderNed"
OCDS_DATA_SET_URI = "https://www.tenderned.nl/cms/nl/aanbesteden-in-cijfers/datasets-aanbestedingen"
PUBLISHER_NAME = "Ministry of Economic Affairs and Climate Policy"

# OCDS Extensions to use

EXTENSIONS = [
    "https://raw.githubusercontent.com/open-contracting-extensions/ocds_coveredBy_extension/master/extension.json",
    "https://raw.githubusercontent.com/open-contracting-extensions/ocds_bidOpening_extension/master/extension.json",
    "https://raw.githubusercontent.com/open-contracting-extensions/ocds_tenderClassification_extension/master/extension.json",
    "https://raw.githubusercontent.com/open-contracting-extensions/ocds_legalBasis_extension/1.1/extension.json",
    "https://raw.githubusercontent.com/open-contracting-extensions/ocds_otherRequirements_extension/master/extension.json",
    "https://raw.githubusercontent.com/open-contracting-extensions/ocds_techniques_extension/master/extension.json",
    "https://raw.githubusercontent.com/open-contracting-extensions/ocds_bid_extension/master/extension.json",
    "https://raw.githubusercontent.com/open-contracting-extensions/ocds_partyDetails_scale_extension/master/extension.json",
    "https://raw.githubusercontent.com/open-contracting-extensions/ocds_lots_extension/master/extension.json",
    "https://bitbucket.org/ONCAETI/ocds_releasesource_extension/raw/master/extension.json",
    "https://raw.githubusercontent.com/open-contracting-extensions/eforms/latest/schema/profile/extension.json",
    "https://raw.githubusercontent.com/open-contracting-extensions/ocds_organizationClassification_extension/1.1/extension.json",
]

# OCDS codes mapping

category_map = {"Leveringen": "goods", "Werken": "works", "Diensten": "services"}

award_map = {
    "Beste prijs-kwaliteit verhouding": "ratedCriteria",
    "Laagste prijs": "priceOnly",
}

procurement_method_map = {
    "Openbaar": "open",
    "Concurrentiegerichte dialoog": "selective",
    "Mededingingsprocedure met onderhandeling": "selective",
    "Onderhandse procedure": "limited",
    "Innovatiepartnerschap": "selective",
    "Niet-openbaar": "selective",
    "Onderhandeling zonder bekendmaking": "limited",
    "Marktconsultatie": None,
}

reserved_participation_map = {"Sociale werkplaats en ondernemers": "shelteredWorkshop"}

# Must match the mapping file columns for buyer and supplier, for example parties/0 is for buyer related columns
buyer_path = "parties/0"
supplier_path = "parties/1"

# Must match the column index in the mapping file. Foe example, bids/statistics/0 is for lowestValidBidValue
bids_details = {
    "lowestValidBidValue": "0",
    "highestValidBidValue": "1",
    "requests": "2",
    "electronicBids": "3",
}


def get_schema():
    with urlopen("https://standard.open-contracting.org/schema/1__1__5/release-schema.json") as f:
        schema = json.load(f)
    with open("local_extensions.json") as local_extension:
        local = json.load(local_extension)
    builder = ProfileBuilder("1__1__5", EXTENSIONS)
    patched_schema = builder.patched_release_schema(schema=schema)
    patched_schema = json_merge_patch.merge(patched_schema, local)
    with open("schema.json", "w") as f:
        json.dump(patched_schema, f, ensure_ascii=False, indent=2)
        f.write("\n")


def text_to_bool(value=None):
    """Convert yes or no to True or False."""
    return {"Ja": value or True, "Nee": None if value else False}


def set_value_when_not_na(data, main_column, new_column, value):
    """Fill new_column with value when main_column is not null."""
    data.loc[~data[main_column].isna(), new_column] = value


def replace_boolean_fields(data):
    """Replace yes or no fields with their boolean format."""
    boolean_fields = [
        "tender/hasParticipationFees",
        "tender/isDigital",
        "tender/techniques/hasElectronicAuction",
        "tender/value/hasTax",
        "awards/value/hasTax",
        "awards/hasSubcontracting",
    ]
    for field in boolean_fields:
        data[field] = data[field].map(text_to_bool())


def year_month_to_days(row):
    """Transform strings with format "X months" or "Y years" into "Z" days."""
    if pd.isna(row):
        return None
    if "Maande" in row:
        # month
        return int(row.split(" Maande")[0]) * 30
    if "Jaren" in row:
        return int(row.split(" Jaren")[0]) * 365
    return None


def set_award_id(row):
    """
    Set award ID.

    - if a supplier id exists, use the supplier id
    - If a supplier id doesn't exist, use the supplier name
    - If a lot id exists, concatenate to the supplier id
    """
    if not pd.isna(row["awards/suppliers/id"]) or not pd.isna(row["awards/suppliers/name"]):
        main_id = (
            (row["awards/suppliers/name"] if pd.isna(row["awards/suppliers/id"]) else row["awards/suppliers/id"])
            + "-"
            + str(row["row_number"])
        )
        if not pd.isna(row["tender/lots/id"]):
            return main_id + "-" + row["tender/lots/id"]
        return main_id
    return None


def set_tag(row):
    """
    Set an OCDS tag. If tender information exist, add tender as tag, if awards information exists, add awards as tags.
    """
    tags = {"tender"}
    if not pd.isna(row["awards/suppliers/id"]):
        tags.add("award")
    return ";".join(tags)


def read_by_years(selected_year=None):
    """Read the file splitting it by years, using Publicatiedatum as the year column."""
    date_column = "Publicatiedatum"
    data = pd.read_excel(FILE_NAME, sheet_name=DATA_SHEET_NAME, dtype=str)
    years = pd.to_datetime(data[date_column], dayfirst=True).dt.year.unique()
    if selected_year:
        if selected_year not in years:
            raise ValueError(f"Data from {selected_year} ({date_column}) doesn't exist in the selected dataset")
        years = [selected_year]
    grouped = data.groupby(pd.to_datetime(data[date_column], dayfirst=True).dt.year)
    for year in years:
        yield str(year), grouped.get_group(year)


def rename_columns(data):
    """Rename the columns from TenderNed to OCDS, according to the mapping in the mapping field."""
    mapping_ocds = pd.read_csv("mapping_ocds.csv")
    mapping_ocds[TENDERNED_SOURCE_COLUMN] = mapping_ocds[TENDERNED_SOURCE_COLUMN].str.upper().str.strip()
    new_column_names = pd.Series(
        mapping_ocds[OCDS_MAP_COLUMN].values,
        index=mapping_ocds[TENDERNED_SOURCE_COLUMN],
    ).to_dict()
    return data.rename(columns=str.upper).rename(columns=new_column_names)


def set_parties_metadata(data):
    set_value_when_not_na(
        data,
        f"{buyer_path}/details/classifications/0/description",
        f"{buyer_path}/details/classifications/0/scheme",
        "TED_CA_TYPE",
    )
    set_value_when_not_na(
        data,
        f"{buyer_path}/details/classifications/1/description",
        f"{buyer_path}/details/classifications/1/scheme",
        "COFOG",
    )
    set_value_when_not_na(
        data,
        f"{buyer_path}/details/classifications/0/description",
        f"{buyer_path}/details/classifications/0/id",
        "1",
    )
    set_value_when_not_na(
        data,
        f"{buyer_path}/details/classifications/1/description",
        f"{buyer_path}/details/classifications/1/id",
        "2",
    )
    data[f"{buyer_path}/id"] = data["buyer/id"]
    data[f"{buyer_path}/name"] = data["buyer/name"]
    data[f"{buyer_path}/roles"] = "buyer"
    data[f"{supplier_path}/id"] = data["awards/suppliers/id"]
    data.loc[
        (pd.isna(data["awards/suppliers/id"])) & (~pd.isna(data["awards/suppliers/name"])),
        "awards/suppliers/id",
    ] = data["awards/suppliers/name"]
    data.loc[
        (pd.isna(data[f"{supplier_path}/id"])) & (~pd.isna(data["awards/suppliers/name"])),
        f"{supplier_path}/id",
    ] = data["awards/suppliers/name"]
    data[f"{supplier_path}/name"] = data["awards/suppliers/name"]
    set_value_when_not_na(data, f"{supplier_path}/name", f"{supplier_path}/roles", "supplier")

    # Due to a quality issue, sometimes a buyer is also a supplier
    data.loc[
        data["awards/suppliers/id"] == data["buyer/id"],
        f"{supplier_path}/roles",
    ] = "buyer;supplier"


def complete_bids_information(data):
    """Complete the metadata information required by OCDS, including bids id and measure name."""
    bid_path = "bids/statistics"
    for key, value in bids_details.items():
        position = int(value)
        bid_id = str(position + 1)
        set_value_when_not_na(data, f"{bid_path}/{position}/value", f"{bid_path}/{position}/id", bid_id)
        set_value_when_not_na(data, f"{bid_path}/{position}/value", f"{bid_path}/{position}/measure", key)
        # Some measures only have currency but not values, we remove that ones
        data.loc[
            data[f"{bid_path}/{position}/value"].isna(),
            f"{bid_path}/{position}/currency",
        ] = None
        # Requests and electronicBids change by lot, so if a lot exists, we use the lot id as part of the bid id
        if key in {"requests", "electronicBids"}:
            data.loc[
                (~data["tender/lots/id"].isna()) & (~data[f"{bid_path}/{position}/value"].isna()),
                f"{bid_path}/{position}/id",
            ] = bid_id + "-" + data["tender/lots/id"]
            data.loc[
                (~data["tender/lots/id"].isna()) & (~data[f"{bid_path}/{position}/value"].isna()),
                f"{bid_path}/{position}/relatedLot",
            ] = data["tender/lots/id"]


def set_subcontracting_percentage(row):
    if not pd.isna(row["awards/subcontracting/value/amount"]) and "%" in str(
        row["awards/subcontracting/value/amount"]
    ):
        return float(row["awards/subcontracting/value/amount"].strip().split("%")[0]) / 100.00
    return None


def delete_non_subcontracting_amounts(row):
    if not pd.isna(row["awards/subcontracting/value/amount"]) and "%" in str(
        row["awards/subcontracting/value/amount"]
    ):
        return None
    return row["awards/subcontracting/value/amount"]


def transform_to_ocds(data):
    data = rename_columns(data)

    format_dates(data)

    data["row_number"] = np.arange(len(data))

    # Code list map
    data["tender/mainProcurementCategory"] = data["tender/mainProcurementCategory"].map(category_map)
    data["tender/awardCriteria"] = data["tender/awardCriteriaDetails"].map(award_map)
    data["tender/procurementMethod"] = data["tender/procurementMethodDetails"].map(procurement_method_map)
    data["tender/otherRequirements/reservedParticipation"] = data[
        "tender/otherRequirements/reservedParticipation"
    ].map(reserved_participation_map)

    replace_boolean_fields(data)

    data["ocid"] = OCID_PREFIX + data["ocid"]

    data["tender/contractPeriod/durationInDays"] = data["tender/contractPeriod/durationInDays"].apply(
        year_month_to_days
    )
    data["tender/contractPeriod/durationInDays"] = data["tender/contractPeriod/durationInDays"].astype("Int64")

    # Boolean to values
    data["tender/coveredBy"] = data["tender/coveredBy"].map(text_to_bool("GPA"))
    data["parties/1/details/scale"] = data["parties/1/details/scale"].map(text_to_bool("sme"))

    # Values to boolean
    data["tender/techniques/hasFrameworkAgreement"] = np.where(
        data["tender/nature"] == "Raamovereenkomst",
        True,  # noqa: FBT003 # false positive
        None,
    )
    data["tender/techniques/hasDynamicPurchasingSystem"] = np.where(
        data["tender/nature"] == "Instellen van dynamisch aankoopsysteem (DAS)",
        True,  # noqa: FBT003 # false positive
        None,
    )
    data["tender/bidOpening/description"] = np.where(
        data["tender/bidOpening/description"] == "Nee",
        "Personen (ondernemers) kunnen niet aanwezig zijn bij de opening van aanmeldingen/inschrijvingen",
        "Personen (ondernemers) mogen aanwezig zijn bij de opening van inschrijvingen/aanbiedingen",
    )

    # OCDS metadata and required IDs
    set_value_when_not_na(data, "tender/classification/description", "tender/classification/scheme", "CPV")
    set_parties_metadata(data)

    data["tender/deliveryAddresses/id"] = "1"

    data["awards/id"] = data.apply(set_award_id, axis=1)

    data["tender/documents/id"] = "1"
    data["tender/documents/documentType"] = "tenderNotice"

    data["awards/relatedLots"] = data["tender/lots/id"]

    set_value_when_not_na(data, "sources/name", "sources/id", "1")
    data["sources/imported"] = None

    data["initiationType"] = "tender"

    data.loc[data["tender/id"].isna(), "tender/id"] = data["id"]

    # Create a unique release id
    data["id"] = data["id"].astype(str) + "-" + data.apply(lambda x: hash(tuple(x)), axis=1).astype(str)

    # Remove non-numeric values from amounts
    data.loc[
        data["awards/subcontracting/value/amount"] == "Onbekend",
        "awards/subcontracting/value/amount",
    ] = None

    # As per https://extensions.open-contracting.org/en/extensions/subcontracting/master/#guidance
    data["awards/subcontracting/minimumPercentage"] = data.apply(set_subcontracting_percentage, axis=1)
    data["awards/subcontracting/maximumPercentage"] = data["awards/subcontracting/minimumPercentage"]

    data["awards/subcontracting/value/amount"] = data.apply(delete_non_subcontracting_amounts, axis=1)

    complete_bids_information(data)

    data["tag"] = data.apply(set_tag, axis=1)

    return data.drop(["row_number"], axis=1)


def convert_to_json(schema, year):
    """Convert a CSV file to OCDS JSON."""
    json_dir = os.path.join(JSON_OUTPUT_DIR, f"{year}.json")
    unflatten(
        os.path.join(CSV_OUTPUT_DIR, year),
        root_list_path="releases",
        root_id="id",
        schema=schema,
        input_format="csv",
        output_name=json_dir,
    )
    return json_dir


def format_dates(data_frame):
    """Format all the date columns as OCDS dates."""
    for column in data_frame.columns:
        if "date" in column.lower():
            data_frame[column] = pd.to_datetime(data_frame[column], dayfirst=True, format="%d-%m-%Y", errors="coerce")
            data_frame[column] = data_frame[column].dt.strftime("%Y-%m-%dT00:00:00Z")


def package_releases(json_dir):
    """Package a list of releases into a release package."""
    with open(json_dir) as f:
        data = json.load(f)
    packages = ocdskit.combine.package_releases(
        data["releases"],
        uri=OCDS_DATA_SET_URI,
        publisher={"name": PUBLISHER_NAME},
        extensions=EXTENSIONS,
        published_date=datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT00:00:00Z"),
    )
    with open(json_dir, "w") as outfile:
        outfile.write(json.dumps(packages))


def initial_setup(*, generate_schema=False):
    """Generate the final OCDS JSON schema to use, and create the paths to use."""
    if generate_schema:
        get_schema()
    for path in [JSON_OUTPUT_DIR, CSV_OUTPUT_DIR]:
        if not os.path.isdir(path):
            os.makedirs(path)


def save_csv(data, year):
    output = os.path.join(CSV_OUTPUT_DIR, year)
    if not os.path.isdir(output):
        os.makedirs(output)
    data.to_csv(os.path.join(output, f"{year}.csv"), index=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int)
    parser.add_argument("--generate-schema", action="store_true")
    args = parser.parse_args()
    initial_setup(generate_schema=args.generate_schema)
    for year, data in read_by_years(selected_year=args.year):
        save_csv(transform_to_ocds(data), year)
        json_dir = convert_to_json("schema.json", year)
        package_releases(json_dir)
    shutil.rmtree(CSV_OUTPUT_DIR)


if __name__ == "__main__":
    main()
