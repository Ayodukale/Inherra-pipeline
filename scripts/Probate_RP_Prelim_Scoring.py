import pandas as pd
import re
from rapidfuzz import fuzz
import jellyfish # For Soundex/Metaphone
from datetime import datetime
import argparse
import numpy as np # For NaN handling and potential numeric ops
import os
import glob
from pathlib import Path

print("--- SCRIPT FILE LOADED BY PYTHON INTERPRETER ---")

# --- Default Paths (can be overridden by command-line args) ---
# --- In Script 3, near the top ---
DEFAULT_INPUT_FOLDER = "Harris RP Data Scrapes/"
DEFAULT_SCRIPT3_OUTPUT_FOLDER = "Script3_Linked_Results/" 
# New base name, will always have a timestamp appended
DEFAULT_FALLBACK_OUTPUT_PREFIX = "script3_output" 

# --- Constants ---
WEIGHTS = {
    'name_last_score': 0.25,
    'name_first_score': 0.10,
    'date_proximity_score': 0.20, # Will be used in Task 5
    'party_role_score': 0.10,     # Will be used in Task 5
    'instrument_weight': 0.15,    # Will be used in Task 5
    'search_tier_weight': 0.10,   # Will be used in Task 5
}

# --- Helper Functions Begin ---



def find_latest_csv_in_folder(folder_path):
    """Finds the most recently modified CSV file in a given folder."""
    print(f"INFO: Searching for latest CSV in folder: {folder_path}")
    if not os.path.isdir(folder_path):
        print(f"ERROR: Input folder not found at '{folder_path}'. Please ensure it exists relative to the script's execution directory or provide an absolute path.")
        return None
    search_pattern = os.path.join(folder_path, '*.csv')
    csv_files = glob.glob(search_pattern)
    if not csv_files:
        print(f"ERROR: No CSV files found in '{folder_path}' matching pattern '{search_pattern}'.")
        return None
    try:
        latest_file = max(csv_files, key=os.path.getmtime)
        print(f"INFO: Automatically selected latest input CSV: {latest_file}")
        return latest_file
    except Exception as e:
        print(f"ERROR: Could not determine the latest file in '{folder_path}': {e}")
        return None

def load_and_parse_dates(csv_path):
    print(f"--- INSIDE load_and_parse_dates FUNCTION ---")
    print(f"INFO: Attempting to load data from {csv_path}...")
    try:
        df = pd.read_csv(csv_path, dtype=str, sep=';') 
        print(f"INFO: Successfully loaded {len(df)} rows and {len(df.columns)} columns.")
        if len(df.columns) <= 1 and len(df) > 0:
            print("CRITICAL WARN: CSV loaded with only 1 or fewer columns. Check separator.")
    except FileNotFoundError:
        print(f"ERROR: File not found at {csv_path}.")
        return None
    except Exception as e:
        print(f"ERROR: Loading {csv_path}: {e}")
        return None
    date_cols_to_parse = {
        'probate_lead_filing_date': '%Y-%m-%d',
        'rp_file_date': '%m/%d/%Y'
    }
    print("INFO: Parsing date columns...")
    for col, date_format_str in date_cols_to_parse.items():
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format=date_format_str, errors='coerce')
        else:
            print(f"WARN: Date column '{col}' not found. Creating with NaT.")
            df[col] = pd.NaT
    print("INFO: Date parsing complete.")
    print(f"--- EXITING load_and_parse_dates FUNCTION ---")
    return df

def clean_name_series(name_series, series_name_for_logging="Name Series"):
    print(f"INFO: Cleaning {series_name_for_logging}...")
    if not isinstance(name_series, pd.Series):
        print(f"ERROR: Input for cleaning '{series_name_for_logging}' must be a pandas Series. Got {type(name_series)}")
        return pd.Series(dtype='object')
    cleaned_series = name_series.fillna('').astype(str).str.upper()
    cleaned_series = cleaned_series.str.replace(r'\s*\b(JR|SR|II|III|IV)\.?$', '', regex=True).str.strip()
    cleaned_series = cleaned_series.str.replace(r'[^\w\s-]', '', regex=True).str.strip()
    cleaned_series = cleaned_series.str.replace(r'\s+', ' ', regex=True).str.strip()
    print(f"INFO: Cleaning for {series_name_for_logging} complete.")
    return cleaned_series

def generate_phonetic_keys(df):
    print(f"--- INSIDE generate_phonetic_keys FUNCTION ---")
    if 'cleaned_probate_lead_decedent_last' in df.columns:
        df['soundex_decedent_last'] = df['cleaned_probate_lead_decedent_last'].apply(
            lambda x: jellyfish.soundex(x) if pd.notna(x) and x.strip() else ''
        )
        print("INFO: Generated Soundex for decedent last names.")
    else:
        df['soundex_decedent_last'] = '' 
    if 'cleaned_rp_party_last_name' in df.columns:
        df['soundex_rp_party_last'] = df['cleaned_rp_party_last_name'].apply(
            lambda x: jellyfish.soundex(x) if pd.notna(x) and x.strip() else ''
        )
        print("INFO: Generated Soundex for RP party last names.")
    else:
        df['soundex_rp_party_last'] = '' 
    print(f"--- EXITING generate_phonetic_keys FUNCTION ---")
    return df

def calculate_name_similarity_scores(df):
    print(f"--- INSIDE calculate_name_similarity_scores FUNCTION ---")
    name_pairs_to_score = [
        ('cleaned_probate_lead_decedent_last', 'cleaned_rp_party_last_name', 'name_last_score'),
        ('cleaned_probate_lead_decedent_first', 'cleaned_rp_party_first_name', 'name_first_score')
    ]
    for col1, col2, score_col_name in name_pairs_to_score:
        if col1 in df.columns and col2 in df.columns:
            print(f"INFO: Calculating fuzz.ratio for {col1} vs {col2} into {score_col_name}")
            df[score_col_name] = df.apply(
                lambda row: fuzz.ratio(str(row[col1]), str(row[col2]))
                            if pd.notna(row[col1]) and pd.notna(row[col2]) else 0,
                axis=1
            )
        else:
            print(f"WARN: One or both columns for name scoring not found: {col1}, {col2}. Skipping {score_col_name}.")
            df[score_col_name] = 0 
    print(f"--- EXITING calculate_name_similarity_scores FUNCTION ---")
    return df

def calculate_date_proximity_score(df):
    """Calculates score based on proximity of rp_file_date and probate_lead_filing_date."""
    print(f"--- INSIDE calculate_date_proximity_score FUNCTION ---")
    score_col_name = 'date_proximity_score'
    if 'rp_file_date' in df.columns and 'probate_lead_filing_date' in df.columns:
        # Ensure dates are datetime objects (should be from load_and_parse_dates)
        # df['rp_file_date'] = pd.to_datetime(df['rp_file_date'], errors='coerce') # Redundant if load_and_parse_dates is robust
        # df['probate_lead_filing_date'] = pd.to_datetime(df['probate_lead_filing_date'], errors='coerce')

        # Calculate absolute difference in days
        # Handle NaT by resulting in NaN for days_apart, then fillna for score
        time_diff = (df['rp_file_date'] - df['probate_lead_filing_date'])
        df['days_apart'] = time_diff.dt.days.abs()

        # Scoring logic based on Chi Chi's blueprint (2 if <180 days, 1 if <365 days)
        # Normalized to 0-100: 100 for <180 days, 50 for <365 days, 0 otherwise.
        conditions = [
            df['days_apart'] < 180,  # Most relevant
            df['days_apart'] < 365   # Still relevant
        ]
        choices = [100, 50] 
        df[score_col_name] = np.select(conditions, choices, default=0)
        df[score_col_name] = df[score_col_name].fillna(0).astype(int) # Handle any NaNs from NaT dates
        print(f"INFO: Calculated {score_col_name}.")
    else:
        print(f"WARN: One or both date columns for proximity calculation not found. Setting {score_col_name} to 0.")
        df[score_col_name] = 0
    print(f"--- EXITING calculate_date_proximity_score FUNCTION ---")
    return df

def calculate_party_role_score(df):
    """Calculates score based on rp_party_type (e.g., Grantor bonus)."""
    print(f"--- INSIDE calculate_party_role_score FUNCTION ---")
    score_col_name = 'party_role_score'
    if 'rp_party_type' in df.columns:
        # Score 100 if Grantor, 0 otherwise for this component (as per Chi Chi's initial features)
        df[score_col_name] = df['rp_party_type'].apply(
            lambda x: 100 if isinstance(x, str) and x.strip().upper() == 'GRANTOR' else 0
        ).astype(int)
        print(f"INFO: Calculated {score_col_name}.")
    else:
        print(f"WARN: 'rp_party_type' column not found. Setting {score_col_name} to 0.")
        df[score_col_name] = 0
    print(f"--- EXITING calculate_party_role_score FUNCTION ---")
    return df

def calculate_instrument_weight(df):
    """Calculates score based on rp_instrument_type."""
    print(f"--- INSIDE calculate_instrument_weight FUNCTION ---")
    score_col_name = 'instrument_weight'
    # Scores 0-100, higher is more indicative of a direct transfer by decedent
    instrument_scores = { 
        "W/D": 100,          # Warranty Deed - Strong indicator
        "DEED": 95,          # General Deed
        "GIFT DEED": 90,
        "QUIT CLAIM DEED": 80, # Less strong than W/D but still a transfer
        "D/T": 30,           # Deed of Trust (Lien, not usually decedent transferring out)
        "RELEASE OF LIEN": 20, # Could be related, but not a primary transfer
        "MODIF": 15,         # Modification
        "NOTICE": 10,        # Notice
        # Add more types and their scores as needed based on relevance
    }
    # Chi Chi's blueprint: 2 if W/D, 1 if NOTICE, 0.5 otherwise.
    # Let's use a slightly more granular 0-100 scale from above for better weighting.
    # We can map Chi Chi's 2,1,0.5 to this scale: e.g. 2->100, 1->50, 0.5->25
    default_instrument_score = 25 # Chi Chi's "0.5 otherwise" mapped to 0-100 scale

    if 'rp_instrument_type' in df.columns:
        df[score_col_name] = df['rp_instrument_type'].apply(
            lambda x: instrument_scores.get(str(x).strip().upper(), default_instrument_score) if pd.notna(x) else default_instrument_score
        ).astype(int)
        print(f"INFO: Calculated {score_col_name}.")
    else:
        print(f"WARN: 'rp_instrument_type' column not found. Setting {score_col_name} to {default_instrument_score}.")
        df[score_col_name] = default_instrument_score
    print(f"--- EXITING calculate_instrument_weight FUNCTION ---")
    return df

def calculate_search_tier_weight(df):
    """Calculates score based on rp_search_tier (how specific the search was)."""
    print(f"--- INSIDE calculate_search_tier_weight FUNCTION ---")
    score_col_name = 'search_tier_weight'
    
    # Scores 0-100, higher is better (more specific search yielding a result)
    tier_scores_map = { 
        "TIER_1": 100, # Most specific search
        "TIER_2": 75,  # Nickname tier or other secondary precise tier
        "TIER_3": 50,  # Least specific (e.g., Last Name only for rare)
    }
    default_tier_score = 25 # For unknown or unlisted tiers

    def get_base_tier(search_tier_str):
        if not isinstance(search_tier_str, str) or not search_tier_str.strip():
            return None # Or some key that maps to default_tier_score
        
        s = search_tier_str.strip().upper()
        if s.startswith("TIER_1"):
            return "TIER_1"
        elif s.startswith("TIER_2"): # This will catch TIER_2_NICK_...
            return "TIER_2"
        elif s.startswith("TIER_3"):
            return "TIER_3"
        return None # Or a key for default

    if 'rp_search_tier' in df.columns:
        df[score_col_name] = df['rp_search_tier'].apply(
            lambda x: tier_scores_map.get(get_base_tier(x), default_tier_score)
        ).astype(int)
        print(f"INFO: Calculated {score_col_name}.")
    else:
        print(f"WARN: 'rp_search_tier' column not found. Setting {score_col_name} to {default_tier_score}.")
        df[score_col_name] = default_tier_score # Ensure column exists
    print(f"--- EXITING calculate_search_tier_weight FUNCTION ---")
    return df

def calculate_match_score_total(df, weights_dict):
    """
    Calculates the total weighted match score based on individual feature scores.
    Individual feature scores are expected to be in the 0-100 range.
    """
    print(f"--- INSIDE calculate_match_score_total FUNCTION ---")
    df['match_score_total'] = 0 # Initialize total score column

    active_weights_sum = 0
    for feature_col, weight in weights_dict.items():
        if feature_col in df.columns:
            print(f"INFO: Applying weight {weight} to feature {feature_col}")
            # Ensure feature column is numeric and handle NaNs by treating them as 0 score for that feature
            df[feature_col] = pd.to_numeric(df[feature_col], errors='coerce').fillna(0)
            df['match_score_total'] += df[feature_col] * weight
            active_weights_sum += weight
        else:
            print(f"WARN: Feature column '{feature_col}' for weighted sum not found in DataFrame. Skipping.")

    # Optional: Normalize the score if the sum of weights used isn't 1.0, to keep it 0-100.
    # This is useful if some features (and their weights) might be missing.
    # However, our feature calculation functions default to creating the column with 0 if source is missing,
    # so all weighted features *should* exist.
    # If active_weights_sum > 0 and active_weights_sum != 1.0:
    #     print(f"INFO: Sum of active weights is {active_weights_sum}. Normalizing total score.")
    #     df['match_score_total'] = (df['match_score_total'] / active_weights_sum) * 100 # Scale to 0-100 if weights don't sum to 1

    # Ensure final score is clipped between 0 and 100
    df['match_score_total'] = np.clip(df['match_score_total'], 0, 100).round(2) # Round to 2 decimal places
    
    print(f"INFO: Calculated match_score_total. Min: {df['match_score_total'].min()}, Max: {df['match_score_total'].max()}")
    print(f"--- EXITING calculate_match_score_total FUNCTION ---")
    return df

def classify_confidence_level(df):
    """
    Classifies the match confidence level based on match_score_total
    and adds a boolean flag for potential matches.
    """
    print(f"--- INSIDE classify_confidence_level FUNCTION ---")
    score_col = 'match_score_total'
    confidence_col = 'match_confidence_level'
    is_match_col = 'is_potential_decedent_match'

    if score_col not in df.columns:
        print(f"ERROR: '{score_col}' not found in DataFrame. Cannot classify confidence. Defaulting.")
        df[confidence_col] = 'Low'
        df[is_match_col] = False
        return df

    # Define conditions and choices for confidence levels
    # Thresholds are based on a 0-100 potential score.
    # Our current max is 90, so "High" might be rare initially. We can tune these.
    conditions = [
        df[score_col] >= 80,  # High confidence
        df[score_col] >= 60   # Medium confidence
    ]
    choices = ['High', 'Medium']
    df[confidence_col] = np.select(conditions, choices, default='Low')

    # Boolean flag for easier filtering of potential matches (High or Medium)
    df[is_match_col] = df[confidence_col].isin(['High', 'Medium'])
    
    print(f"INFO: Classified confidence levels. Counts: \n{df[confidence_col].value_counts()}")
    print(f"INFO: Flagged potential matches. True: {df[is_match_col].sum()}, False: {len(df) - df[is_match_col].sum()}")
    print(f"--- EXITING classify_confidence_level FUNCTION ---")
    return df


# --- Helper Functions End ---

def main(input_arg, output_arg):
    print("--- INSIDE MAIN FUNCTION: PROCESSING STARTED ---") 
    
    # --- Step 1: Determine actual input and output paths ---
    actual_input_csv_path = None
    # Check if input_arg is None, an empty string, or "LATEST" (case-insensitive)
    if input_arg is None or str(input_arg).strip().upper() == "LATEST" or str(input_arg).strip() == "":
        print(f"INFO: No specific input CSV provided or 'latest' requested. Searching in default folder: {DEFAULT_INPUT_FOLDER}")
        actual_input_csv_path = find_latest_csv_in_folder(DEFAULT_INPUT_FOLDER)
        if not actual_input_csv_path:
            print("ERROR: Could not find an input CSV to process. Exiting.")
            return # Exit if no input file found
    else:
        actual_input_csv_path = input_arg

    actual_output_csv_path = None
    # Check if output_arg is None, an empty string, or "DEFAULT_OUTPUT" (case-insensitive)
    if output_arg is None or str(output_arg).strip().upper() == "DEFAULT_OUTPUT" or str(output_arg).strip() == "":
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Ensure the default output folder exists
        try:
            Path(DEFAULT_SCRIPT3_OUTPUT_FOLDER).mkdir(parents=True, exist_ok=True)
        except Exception as e_mkdir:
            print(f"ERROR: Could not create default output folder {DEFAULT_SCRIPT3_OUTPUT_FOLDER}. Error: {e_mkdir}. Defaulting to current directory for output.")
            # Fallback to current directory if default folder creation fails
            current_dir_for_output = "." 
            if actual_input_csv_path and isinstance(actual_input_csv_path, str): 
                input_basename = os.path.splitext(os.path.basename(actual_input_csv_path))[0]
                filename = f"{input_basename}_linked_{timestamp}.csv"
            else: 
                filename = f"{DEFAULT_FALLBACK_OUTPUT_PREFIX}_{timestamp}.csv"
            actual_output_csv_path = os.path.join(current_dir_for_output, filename)
        else: # Default folder creation was successful or folder already existed
            if actual_input_csv_path and isinstance(actual_input_csv_path, str): 
                input_basename = os.path.splitext(os.path.basename(actual_input_csv_path))[0]
                filename = f"{input_basename}_linked_{timestamp}.csv"
            else: 
                filename = f"{DEFAULT_FALLBACK_OUTPUT_PREFIX}_{timestamp}.csv"
            actual_output_csv_path = os.path.join(DEFAULT_SCRIPT3_OUTPUT_FOLDER, filename)
        
        print(f"INFO: No specific output CSV path provided or 'default' requested. Using: {actual_output_csv_path}")
    else: # An output path was provided as an argument
        output_dir_from_arg = os.path.dirname(output_arg)
        if output_dir_from_arg and not os.path.exists(output_dir_from_arg):
            try:
                Path(output_dir_from_arg).mkdir(parents=True, exist_ok=True)
                print(f"INFO: Created output directory specified in argument: {output_dir_from_arg}")
            except Exception as e_mkdir_arg:
                print(f"ERROR: Could not create output directory {output_dir_from_arg} from argument. Error: {e_mkdir_arg}. Attempting to save in current directory.")
                # Fallback: Try to save just the filename part in the current directory
                actual_output_csv_path = os.path.basename(output_arg) 
                print(f"WARN: Saving output to current directory as: {actual_output_csv_path}")
        actual_output_csv_path = output_arg # Use the user-provided path
    # --- End of determining paths ---

    print(f"--- Starting Script 3 V1.0: Record Linkage ---")
    print(f"Processing Input CSV: {actual_input_csv_path}") 
    print(f"Will save final Output to CSV (Task 8 target): {actual_output_csv_path}") 

    # --- Task 2: Load and parse dates ---
    df = load_and_parse_dates(actual_input_csv_path) 
    if df is None or df.empty:
        print("ERROR: Data loading failed or DataFrame is empty in main. Exiting.")
        return

    # --- Task 3: Name Cleaning and Phonetics ---
    print("INFO: Starting name cleaning process...")
    name_columns_to_clean_map = {
        'probate_lead_decedent_first': 'cleaned_probate_lead_decedent_first',
        'probate_lead_decedent_last': 'cleaned_probate_lead_decedent_last',
        'rp_party_first_name': 'cleaned_rp_party_first_name',
        'rp_party_last_name': 'cleaned_rp_party_last_name'
    }
    for original_col, cleaned_col_name in name_columns_to_clean_map.items():
        if original_col in df.columns:
            df[cleaned_col_name] = clean_name_series(df[original_col], series_name_for_logging=original_col)
        else:
            print(f"WARN: Original name column '{original_col}' not found for cleaning. '{cleaned_col_name}' will be empty.")
            df[cleaned_col_name] = pd.Series(dtype='object') 
    df = generate_phonetic_keys(df)
    print("INFO: Name cleaning and phonetic key generation complete.")
    
    # --- Task 4: Name Similarity Scores ---
    df = calculate_name_similarity_scores(df)

    # --- Task 5: Other Feature Scores ---
    print("INFO: Calculating additional feature scores...")
    df = calculate_date_proximity_score(df)
    df = calculate_party_role_score(df)
    df = calculate_instrument_weight(df)
    df = calculate_search_tier_weight(df)
    print("INFO: Additional feature score calculation complete.")

    # --- Task 6: Calculate Total Match Score ---
    df = calculate_match_score_total(df, WEIGHTS)
    
    # --- Task 7: Classify Confidence Levels and Flag Matches ---
    print("INFO: Starting confidence level classification...")
    df = classify_confidence_level(df)
    print("INFO: Confidence level classification complete.")

    # --- Reordering and Saving for Task 7 Check (This will become Task 8 for final save) ---
    print("INFO: Preparing Task 7 check file with reordered columns...")
    desired_column_order = [
        # I. Linkage & Score Information
        'match_score_total', 'match_confidence_level', 'is_potential_decedent_match',
        'name_last_score', 'name_first_score', 
        'date_proximity_score', 'party_role_score', 'instrument_weight', 'search_tier_weight', 
        # II. RP Legal Description
        'rp_legal_description_text', 'rp_legal_lot', 'rp_legal_block', 
        'rp_legal_subdivision', 'rp_legal_abstract', 'rp_legal_survey', 
        'rp_legal_tract', 'rp_legal_sec',
        # III. Matched Party Information
        'rp_party_type', 'rp_party_last_name', 'rp_party_first_name',
        'cleaned_rp_party_last_name', 'cleaned_rp_party_first_name', 'soundex_rp_party_last',
        # IV. RP Document Information
        'rp_file_number', 'rp_file_date', 'days_apart', 'rp_instrument_type',
        'rp_signal_strength', 'rp_found_by_search_term', 'rp_search_tier',
        # V. Original Probate Lead Information
        'probate_lead_decedent_last', 'probate_lead_decedent_first',
        'cleaned_probate_lead_decedent_last', 'cleaned_probate_lead_decedent_first', 'soundex_decedent_last',
        'probate_lead_filing_date', 'probate_lead_case_number', 'probate_lead_county',
        'probate_lead_type_desc', 'probate_lead_subtype', 'probate_lead_status',
        'probate_lead_signal_strength'
    ]
    
    all_current_columns = df.columns.tolist()
    final_ordered_columns = []
    remaining_columns = all_current_columns[:] 

    for col in desired_column_order:
        if col in remaining_columns:
            final_ordered_columns.append(col)
            remaining_columns.remove(col) 
    final_ordered_columns.extend(remaining_columns)

    df_to_save = df # Start with the full df
    try:
        df_ordered = df[final_ordered_columns]
        df_to_save = df_ordered # If reordering is successful, use the ordered df
    except KeyError as e:
        print(f"ERROR: KeyError during column reordering: {e}. Problematic columns might be in 'desired_column_order' but not in DataFrame. Saving with original column order.")
        # df_to_save remains df (original order)
    
    # For V1.0, Task 8 is to save the final sorted output, and Task 9 is the QA sample.
    # Let's make the _task7_check.csv the same as the final output for now (minus QA sampling)
    
    # Sort by 'match_score_total' in descending order (Part of Task 8)
    print("INFO: Sorting DataFrame by 'match_score_total' descending...")
    # Use df_to_save which is either df_ordered or df (fallback)
    df_sorted = df_to_save.sort_values(by='match_score_total', ascending=False)
    
    # Save the "final" (pre-QA sample) output (Task 8)
    try:
        # actual_output_csv_path was determined at the beginning of main()
        df_sorted.to_csv(actual_output_csv_path, index=False, sep=';')
        print(f"INFO: (V1.0 Final Output) Enriched and sorted data saved to {actual_output_csv_path}")
        print(f"INFO: Please inspect '{actual_output_csv_path}'.")
    except Exception as e:
        print(f"ERROR: Could not save final output CSV to {actual_output_csv_path}. Error: {e}")

    # --- Task 9: Prep 25-row sample file for manual QA ---
    if not df_sorted.empty: # Use the sorted dataframe for QA sampling
        print("INFO: Preparing QA sample file...")
        # ... (QA Sampling logic from before, using df_sorted as input) ...
        qa_sample_dfs = []
        high_matches = df_sorted[df_sorted['match_confidence_level'] == 'High']
        medium_matches = df_sorted[df_sorted['match_confidence_level'] == 'Medium']
        low_matches = df_sorted[df_sorted['match_confidence_level'] == 'Low']

        qa_sample_dfs.append(high_matches.head(10))
        remaining_slots = 25 - len(qa_sample_dfs[0])
        if remaining_slots > 0: qa_sample_dfs.append(medium_matches.head(min(8, remaining_slots)))
        current_sample_count = sum(len(s_df) for s_df in qa_sample_dfs)
        remaining_slots = 25 - current_sample_count
        if remaining_slots > 0: qa_sample_dfs.append(low_matches.head(min(7, remaining_slots)))
        current_sample_count = sum(len(s_df) for s_df in qa_sample_dfs)
        if current_sample_count < 25 and len(df_sorted) > current_sample_count:
            additional_needed = 25 - current_sample_count
            # Simple way to get more to fill up to 25 from remaining low_matches
            # This needs to be careful not to re-select. A better way is to concat and then drop_duplicates.
            # For now, this will mostly work if there are enough distinct lows.
            start_index_low = len(qa_sample_dfs[2]) if len(qa_sample_dfs) > 2 and qa_sample_dfs[2] is low_matches.head(min(7,remaining_slots+additional_needed)) else 0
            qa_sample_dfs.append(low_matches.iloc[start_index_low : start_index_low + additional_needed])
            
        qa_sample_df = pd.concat(qa_sample_dfs).drop_duplicates().head(25)

        if qa_sample_df.empty and not df_sorted.empty : 
             qa_sample_df = df_sorted.head(min(25, len(df_sorted)))

        if not qa_sample_df.empty:
            qa_sample_output_path = actual_output_csv_path.replace(".csv", "_QA_SAMPLE.csv")
            try:
                qa_sample_df.to_csv(qa_sample_output_path, index=False, sep=';')
                print(f"INFO: QA sample data (up to 25 rows, stratified) saved to {qa_sample_output_path}")
            except Exception as e:
                print(f"ERROR: Could not save QA sample CSV to {qa_sample_output_path}. Error: {e}")
        else:
            print("WARN: Not enough varied data to create a meaningful QA sample, or DataFrame was empty.")
    else:
        print("WARN: DataFrame is empty, skipping QA sample generation.")
        
    print("--- SCRIPT V1.0 MAIN FUNCTION FULLY COMPLETED (Tasks 1-9) ---")

if __name__ == "__main__":
    print("--- SCRIPT EXECUTION STARTED (IF __NAME__ == MAIN BLOCK) ---") 
    parser = argparse.ArgumentParser(description="Script 3: Advanced Record Linkage & Relevance Scoring V1.0")
    
    parser.add_argument("--input", "-i", default=None,
                        help="Path to the specific input CSV file (output from Script 2). "
                             "If omitted, script searches DEFAULT_INPUT_FOLDER for the latest.")
    parser.add_argument("--output", "-o", default=None,
                        help="Path to save the final enriched output CSV file. "
                             "If omitted, a default name will be generated based on input or timestamp.")
    
    args = parser.parse_args()
    print(f"--- ARGS PARSED: Input='{args.input}', Output='{args.output}' ---") 
    
    main(args.input, args.output)
    print("--- SCRIPT EXECUTION FINISHED (IF __NAME__ == MAIN BLOCK) ---")