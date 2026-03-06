"""
Brands classification script - converted from brands_classification.ipynb.
Classifies customer names into brands with parent company and organization type.
"""

import pandas as pd
from pathlib import Path
from thefuzz import fuzz

# Paths (relative to project root)
BASE = Path(__file__).resolve().parent.parent
MASTER_PATH = BASE / "datasets" / "2_master_data" / "master_data.csv"
INPUT_PATH = BASE / "datasets" / "3_input_data" / "customer_list.xlsx"
OUTPUT_PATH = BASE / "datasets" / "4_output_data" / "customer_classified.csv"

MATCH_THRESHOLD = 90


def classify_customer(name: str, lookup_df: pd.DataFrame) -> tuple[str, str, str]:
    """Classify a customer name against master data. Returns (group_name, parent_name, org_type)."""
    name = str(name).strip() if pd.notna(name) else ""
    if not name:
        return "Unknown", "Unknown", "Other"

    df_match = lookup_df.copy()
    df_match["ratio"] = df_match["group_name"].apply(
        lambda x: fuzz.partial_token_sort_ratio(str(x).lower().strip(), name.lower().strip())
    )
    df_match = df_match.sort_values(by=["ratio"], ascending=False)
    top_row = df_match.iloc[0]

    if top_row["ratio"] >= MATCH_THRESHOLD:
        return (
            top_row["group_name"],
            top_row["parent_company"],
            top_row["organization_type"],
        )
    return "Unknown", "Unknown", "Other"


def main():
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)
    pd.set_option("display.expand_frame_repr", False)

    if not INPUT_PATH.exists():
        print(f"Input file not found: {INPUT_PATH}")
        print("Create datasets/3_input_data/ and add customer_list.xlsx with a 'customer_name' column.")
        return

    lookup_df = pd.read_csv(MASTER_PATH)
    df = pd.read_excel(INPUT_PATH)

    print(df.info())

    # Use customer_name column (handles both customer_name and Customer Name)
    name_col = "customer_name" if "customer_name" in df.columns else "Customer Name"
    new_list = df[name_col].tolist()

    results = []
    for name in new_list:
        group_name, parent_name, org_type = classify_customer(name, lookup_df)
        results.append({
            "Customer Name": name,
            "Group Name": group_name,
            "Parent Name": parent_name,
            "Organization Type": org_type,
        })

    classified_df = pd.DataFrame(results)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    classified_df.to_csv(OUTPUT_PATH, index=False)

    print(f"\nOutput saved to: {OUTPUT_PATH}")
    print("\nFinal Classification Sample:")
    print(classified_df.head())


if __name__ == "__main__":
    main()
