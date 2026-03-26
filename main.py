import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

class Thor_ww2_db:

    def __init__(self, file_path):
        self.file_path = file_path
        self.clean_file = None


    def extract_data(self):
        # Load raw data and extract relevant columns
        temp_df = pd.read_csv(self.file_path, on_bad_lines='skip', low_memory=False)
        self.clean_file = temp_df[["Mission ID", "Mission Date", "Country", "Target Country", 
            "Aircraft Series", "High Explosives Weight (Tons)", 
            "Incendiary Devices Weight (Tons)", "Fragmentation Devices Weight (Tons)", 
            "Total Weight (Tons)"]].copy()
        
        # Set Mission ID as the primary index
        self.clean_file.set_index("Mission ID", inplace=True)
        
    
    def clean_data(self):

        # Standardize column names for SQL compatibility
        self.clean_file.columns = (self.clean_file.columns.str.strip().str.lower().str.replace(' ', '_').str.replace(r'[^\w\s]', '', regex=True))

        # Creating mission_type based on the target_country column, using the "MINING" wrote next to the target country name
        self.clean_file['mission_type'] = self.clean_file['target_country'].apply(lambda x: 'MINING' if 'mining' in str(x).lower() else 'BOMBING')

        # Parse dates, sort chronologically, and handle missing baseline data
        self.clean_file['mission_date'] = pd.to_datetime(self.clean_file['mission_date'], errors='coerce')
        self.clean_file = self.clean_file.sort_values(by='mission_date')
        self.clean_file['country'] = self.clean_file['country'].fillna('UNKNOWN')

        # Remove records without valid aircraft identifiers
        self.clean_file = self.clean_file.dropna(subset=['aircraft_series'])
        aircraft_to_remove = ['100 SQ', '35 FG', '43 BG', 'HVY', 'MED', 'LGT', 'BOM', 'BATT', 'UNKNOWN']
        self.clean_file = self.clean_file[~self.clean_file['aircraft_series'].isin(aircraft_to_remove)]

        # Drop records where all ordnance weight columns are completely empty
        bombs_columns_dropna= ['high_explosives_weight_tons', 
            'incendiary_devices_weight_tons', 
            'fragmentation_devices_weight_tons',
            'total_weight_tons']
        self.clean_file = self.clean_file.dropna(subset=bombs_columns_dropna, how='all')


        # HISTORICAL DATA CORRECTIONS
        # Correct payload weights for atomic bomb missions (Hiroshima and Nagasaki)

        #Hiroshima
        condition1 = (self.clean_file['mission_date'] == '1945-08-06' ) & \
             (self.clean_file['country'] == 'USA') & \
             (self.clean_file['target_country'] == 'JAPAN') & \
             (self.clean_file['high_explosives_weight_tons'] == 15000)
        
        # Nagasaki
        condition2 = (self.clean_file['mission_date'] == '1945-08-09') & \
             (self.clean_file['country'] == 'USA') & \
             (self.clean_file['target_country'] == 'JAPAN') & \
             (self.clean_file['high_explosives_weight_tons'] == 20000)
        
        # Update both explosive and total weight columns
        self.clean_file.loc[condition1, ['high_explosives_weight_tons', 'total_weight_tons']] = 5
        self.clean_file.loc[condition2, ['high_explosives_weight_tons', 'total_weight_tons']] = 5


        # --- PAYLOAD MATH & LOGIC ---
        # Calculate the mathematical sum of individual ordnance columns
        bomb_columns = [
            'high_explosives_weight_tons', 
            'incendiary_devices_weight_tons', 
            'fragmentation_devices_weight_tons'
        ]
        calculated_sum = self.clean_file[bomb_columns].sum(axis=1)
        

        # Fill missing total weights with the calculated sum; otherwise, keep original total
        self.clean_file['total_weight_tons'] = np.where(
            self.clean_file['total_weight_tons'].fillna(0) == 0, 
            calculated_sum, 
            self.clean_file['total_weight_tons']
        )


        # EXCEPTION REPORTING: HISTORICAL ANOMALIES

        # Identify records where the calculated sum of individual bombs (>0) 
        # conflicts with the historically recorded total weight
        anomalies_condition = (calculated_sum > 0) & (calculated_sum.round(2) != self.clean_file['total_weight_tons'].fillna(0).round(2))
        anomalies_df = self.clean_file[anomalies_condition]
        
        # Exporting discrepancies to a separate CSV to preserve historical data integrity 
        # and allow for future domain-expert review, without altering original source values
        if not anomalies_df.empty:
            anomalies_df.to_csv('historical_weight_anomalies.csv', index=True)


        # STRING NORMALIZATION

        # Dictionary mapping for cleaning and standardizing target country names
        country_cleaning_map = {
            r'(?i)\s+MINING': '', 
            r'(?i)THAILAND.*|SIAM.*': 'THAILAND',
            r'(?i)THAILAND OR SIAM.*': 'THAILAND',
            r'(?i)KOREA OR CHOSEN.*': 'KOREA',
            r'(?i)HOLLAND OR NETHERLANDS.*': 'NETHERLANDS',
            r'(?i)ETHIOPIA/ABSINNYA.*': 'ETHIOPIA',
            r'(?i)PHILIPPINE ISLANDS.*': 'PHILIPPINES',
            r'(?i)SICILY|SARDINIA|PANTELLARIA': 'ITALY',
            r'(?i)UNKNOWN OR NOT INDICATED': 'UNKNOWN',
            r'(?i).*PAPUA.*|.*MANUS ISLAND.*|.*NEW GUINEA.*': 'PAPUA NEW GUINEA',
            r'(?i)FORMOSA.*': 'TAIWAN',
        }
        self.clean_file['target_country'] = self.clean_file['target_country'].replace(country_cleaning_map, regex=True).str.strip()

        # Clean and standardize aircraft nomenclature
        aircraft_cleaning = {
            r'(?i)WHIT': 'WHITLEY', r'(?i)BLEN': 'BLENHEIM', r'(?i)0B17': 'B17', r'(?i)0B24': 'B24', r'(?i)ALBA': 'ALBACORE', r'(?i)AUDA': 'AUDAX', r'(?i)LB30': 'B24', r'(?i)LIB': 'B24', r'(?i)P400': 'P39', r'(?i)P401': 'P40',
            r'(?i)BOST.*': 'A20', r'(?i)TBF AVENGER': 'TBF', r'(?i)SBD DAUNTLESS': 'SBD', r'(?i)VENGEANCE \(A31\)': 'A31', r'(?i)TOM': 'P40', r'(?i)MOHAWK': 'P36', r'(?i)PV-1 VENTURA': 'PV1', r'(?i)OB17':'B17',
            r'(?i)OB24': 'B24', r'(?i)HURR': 'HURRICANE', r'(?i)HALI': 'HALIFAX', r'(?i)STIR': 'STIRLING', r'(?i)WELL': 'WELLINGTON', r'(?i)HAMP': 'HAMPDEN', r'(?i)BALT': 'BALTIMORE', r'(?i)MARY': 'MARYLAND',
            r'(?i)MANC': 'MANCHESTER', r'(?i)BEAUF': 'BEAUFORT', r'(?i)\bBEAU\b': 'BEAUFIGHTER', r'(?i)SWORD': 'SWORDFISH', r'(?i)F06': 'F6', r'(?i)VALE': 'VALENTIA', r'(?i)P45': 'P39',
            r'(?i)236/330 WINGS': 'WELLINGTON'
            }
        self.clean_file['aircraft_series'] = self.clean_file['aircraft_series'].replace(aircraft_cleaning, regex=True).str.strip()
        self.clean_file['aircraft_series'] = self.clean_file['aircraft_series'].str.replace(r'([A-Z])([0-9])', r'\1-\2', regex=True)


    def load_to_postgres(self):
        
        load_dotenv()
        
        db_password = os.getenv("DB_password")
        if not db_password:
            raise ValueError("ERROR: Password not found!")

        uri = f"postgresql://postgres:{db_password}@localhost:5432/thor_ww2_db"
        engine = create_engine(uri)

        self.clean_file.to_sql(
            name='ww2_missions',
            con=engine,
            if_exists='replace',
            index=True,
            index_label='mission_id'
        )

if __name__ == "__main__":
    pipeline = Thor_ww2_db('operations.csv')
    pipeline.extract_data()
    pipeline.clean_data()
    pipeline.load_to_postgres()