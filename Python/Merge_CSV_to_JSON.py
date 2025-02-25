import pandas as pd
import json


def merge_csv_into_json(csv_file_path, existing_json_file_path, new_json_file_path):
   # Read CSV file into a pandas DataFrame
   df_csv = pd.read_csv(csv_file_path)


   # Read existing JSON file
   try:
       with open(existing_json_file_path, 'r') as json_file:
           existing_json_data = json.load(json_file)
   except FileNotFoundError:
       existing_json_data = {}


   # Check if "supplements" nest exists in existing JSON
   if "supplements" in existing_json_data:
       # Replace "supplements" with data from CSV file
       existing_json_data["supplements"] = {}


   # Filter CSV data based on 'propid' in the existing JSON
   matching_csv_data = df_csv[df_csv["propid"] == existing_json_data.get("propid")]


   # Create a new JSON structure similar to the original
   new_json_data = existing_json_data.copy()


   # Initialize the "supplements" nest if it doesn't exist
   new_json_data.setdefault("supplements", {})


   # Loop through matching CSV data and insert into "supplements" nest
   for _, row in matching_csv_data.iterrows():
       supplement_id = row["supplement_id"]


       # If "field_name" is blank, use the column name with data for the "propid"
       field_name = row["field_name"] if row["field_name"] and not pd.isna(row["field_name"]) else df_csv.columns[df_csv.columns != "propid"][0]


       if supplement_id not in new_json_data["supplements"]:
           new_json_data["supplements"][supplement_id] = {
               "alt_id": row["alt_id"],
               "status": row["status"],
               "subsections": {row["section_name"]: row["section_status"]},
               "fields": {field_name: []}
           }


       # Add subsections based on CSV data
       new_json_data["supplements"][supplement_id]["subsections"][row["section_name"]] = row["section_status"]


       # Add CSV data to the "fields" nest under the corresponding supplement_id
       new_json_data["supplements"][supplement_id]["fields"].setdefault(field_name, [])


       # Include any non-blank or non-null values from the CSV cells
       field_data = {col: row[col] for col in df_csv.columns if col not in ["alt_id", "supplement_id", "section_name", "section_status", "field_name", "status", "propid"]
                     and (row[col] is not None) and (str(row[col]).strip() != '') and not pd.isna(row[col])}


       # Check if values are not empty before appending to the JSON
       if any(value for value in field_data.values()):
           new_json_data["supplements"][supplement_id]["fields"][field_name].append(field_data)


   # Save the updated JSON to a new file
   with open(new_json_file_path, 'w') as json_file:
       json.dump(new_json_data, json_file, indent=2)


if __name__ == "__main__":
   # Replace with the path to your CSV file
   csv_file_path = "/Users/mmreibe/Documents/test.csv"


   # Replace with the path to your existing JSON file
   existing_json_file_path = "/Users/mmreibe/Documents/jsontest.json"


   # Replace with the desired path for the output JSON file
   new_json_file_path = "/Users/mmreibe/Documents/supplement.json"


   # Merge CSV into existing JSON
   merge_csv_into_json(csv_file_path, existing_json_file_path, new_json_file_path)


   print(f"Merging complete. Updated JSON file saved at: {new_json_file_path}")