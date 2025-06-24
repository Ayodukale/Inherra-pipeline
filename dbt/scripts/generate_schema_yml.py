import snowflake.connector
import yaml
import os
import argparse
from dotenv import load_dotenv

# --- Configuration & Setup ---

# This script assumes you have a .env file in your project root for credentials.
# This keeps secrets out of your code.
#
# Create a file named ".env" and add your details:
# SNOWFLAKE_USER="INHERRA_SERVICE_USER"
# SNOWFLAKE_PRIVATE_KEY_PATH="~/.ssh/inherra_key.p8"
# SNOWFLAKE_ACCOUNT="aec94635.us-east-1"
# SNOWFLAKE_ROLE="DBT_ROLE"  # <-- The role that OWNS the schema
# SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
# SNOWFLAKE_DATABASE="RAW_DATA_DB" # <-- The DB we discovered
# SNOWFLAKE_SCHEMA="dbt_aodukale"   # <-- The schema we discovered

load_dotenv()

# --- Script Constants ---
# These are the hardcoded paths and names for our script.
SCHEMA_FILE_PATH = '../models/staging/schema.yml' # Path relative to the scripts/ folder
MODEL_NAME = 'stg_probate_filings_cleaned'
REFERENCE_TABLE = 'REFERENCE__JSON_KEYS'


# --- Core Logic ---

def get_snowflake_connection():
    """Establishes a Snowflake connection using environment variables."""
    try:
        # This check prevents the "str | None" error
        private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
        if not private_key_path:
            raise ValueError("SNOWFLAKE_PRIVATE_KEY_PATH environment variable is not set in your .env file")
        
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            private_key_file=os.path.expanduser(private_key_path), # Now this is guaranteed to be a string
            role=os.getenv("SNOWFLAKE_ROLE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to Snowflake: {e}")
        raise

def get_schema_from_source(conn):
    """Fetches column metadata from the enhanced reference table."""
    # Note: We use uppercase for the table name to be safe, though Snowflake
    # should handle it if the connection context is right.
    query = f"""
    SELECT 
        COLUMN_NAME, 
        DESCRIPTION, 
        IS_NULLABLE,
        CUSTOM_TESTS
    FROM {REFERENCE_TABLE}
    ORDER BY COLUMN_NAME;
    """
    print(f"Executing query:\n{query}")
    with conn.cursor() as cur:
        cur.execute(query)
        # Return a dictionary for easy lookups, with names normalized to lowercase
        source_cols = {}
        for row in cur.fetchall():
            col_name, desc, is_nullable, custom_tests = row
            
            tests = []
            if not is_nullable:
                tests.append('not_null')
            
            # This handles if custom_tests is a real ARRAY or a string representation
            if custom_tests:
                # If it's a string like "['unique']", eval it. Risky but sometimes necessary.
                if isinstance(custom_tests, str):
                    try:
                        tests.extend(eval(custom_tests))
                    except:
                        print(f"⚠️  Could not parse custom_tests string for {col_name}: {custom_tests}")
                else: # Assume it's a list/array
                    tests.extend(custom_tests)

            source_cols[col_name.lower()] = {
                'name': col_name,
                'description': desc or '', # Ensure description is not None
                'tests': tests
            }
        return source_cols

def get_existing_schema(path):
    """Loads the existing schema.yml file if it exists."""
    absolute_path = os.path.join(os.path.dirname(__file__), path)
    if not os.path.exists(absolute_path):
        print("No existing schema.yml found. A new file will be created.")
        return {}
    
    with open(absolute_path, 'r') as f:
        try:
            return yaml.safe_load(f) or {} # Return empty dict if file is empty
        except yaml.YAMLError as e:
            print(f"⚠️  Could not parse existing schema.yml: {e}. Will create a new file.")
            return {}

def merge_schemas(existing_schema, source_cols, model_name):
    """Merges the source-of-truth schema with the existing manual schema."""
    # Find the target model in the existing YAML
    models_list = existing_schema.get('models', [])
    model_def = next((m for m in models_list if m and m.get('name') == model_name), None)

    if not model_def:
        model_def = {
            'name': model_name,
            'description': f'Schema auto-generated from {REFERENCE_TABLE}',
            'columns': []
        }

    existing_cols = {col['name'].lower(): col for col in model_def.get('columns', [])}
    final_cols = []

    all_col_names = sorted(list(set(existing_cols.keys()) | set(source_cols.keys())))

    for col_name_lower in all_col_names:
        source = source_cols.get(col_name_lower)
        existing = existing_cols.get(col_name_lower)

        if source and existing:
            # Both exist: merge them. Prioritize source for tests, but keep manual descriptions if source is empty.
            final_col = existing.copy()
            final_col['description'] = source['description'] or existing.get('description', '')
            final_col['tests'] = source['tests']
            final_cols.append(final_col)
        elif source and not existing:
            # New column from source: add it
            final_cols.append(source)
            print(f"➕ Added new column: {source['name']}")
        elif existing and not source:
            # Old column, no longer in source: keep it, but add a warning.
            print(f"⚠️  Column '{existing['name']}' exists in schema.yml but not in the source table. It will be kept.")
            final_cols.append(existing)
            
    model_def['columns'] = final_cols
    
    # Rebuild the final YAML structure to ensure our model is present
    # and other models are preserved
    other_models = [m for m in models_list if m and m.get('name') != model_name]
    final_yaml = {'version': 2, 'models': other_models + [model_def]}
        
    return final_yaml

def write_yml_file(yml_content, path, dry_run=False):
    """Writes the YAML content to a file with proper dbt formatting."""
    class DbtYamlDumper(yaml.SafeDumper):
        def increase_indent(self, flow=False, indentless=False):
            return super(DbtYamlDumper, self).increase_indent(flow, False)

    output = yaml.dump(
        yml_content, 
        Dumper=DbtYamlDumper,
        sort_keys=False, 
        default_flow_style=False,
        indent=2,
        width=1000
    )

    if dry_run:
        print("\n--- DRY RUN: The following YAML would be generated ---\n")
        print(output)
        return
    
    absolute_path = os.path.join(os.path.dirname(__file__), path)
    os.makedirs(os.path.dirname(absolute_path), exist_ok=True)
    with open(absolute_path, 'w') as f:
        f.write(output)
    print(f"✅ Schema at '{absolute_path}' successfully updated/merged.")


def main():
    parser = argparse.ArgumentParser(description="Generate/merge dbt schema.yml from a Snowflake reference table.")
    parser.add_argument("--dry-run", action="store_true", help="Print the generated YAML to console instead of writing to file.")
    args = parser.parse_args()

    conn = None
    try:
        conn = get_snowflake_connection()
        source_cols = get_schema_from_source(conn)
        print(f"Found {len(source_cols)} columns in the source table.")
        
        existing_schema = get_existing_schema(SCHEMA_FILE_PATH)
        
        final_yaml = merge_schemas(existing_schema, source_cols, MODEL_NAME)
        
        write_yml_file(final_yaml, SCHEMA_FILE_PATH, dry_run=args.dry_run)

    except Exception as e:
        print(f"❌ An error occurred: {e}")
        exit(1)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()