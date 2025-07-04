# Nama File: model.py

import pandas as pd
import os
import pathlib
from datetime import datetime

class SqlGeneratorModel:
    def __init__(self, source_filepath: str, baseline_filepath: str):
        self.source_path = source_filepath
        self.baseline_path = baseline_filepath
        self.output_sql_file = 'generated_sql_script.sql'
        self._col_source_rm = 'pn_relationship_officer'
        self._col_source_rekening = 'account_number'
        self._col_source_cif = 'ciff_no'
        self._col_source_nama_klien = 'short_name'
        self._col_source_balance = 'balance'
        self._col_source_avail_balance = 'available_balance'
        self._col_source_currency = 'curr code'
        self._col_source_prod_code = 'prod code'
        self._col_date = 'periode'
        self._col_baseline_pn = 'PN'
        self._col_baseline_nama_rm = 'Nama'
        self._col_baseline_rekening = 'Rekening'
        self.branch_name = 'KC Surabaya Kaliasin'
        self.branch_address = 'BRI Cabang Surabaya Kaliasin, Gd. BRI Tower, No. 122-138, Jl. Basuki Rahmat, Embong Kaliasin, Kec. Genteng, Surabaya, Jawa Timur 60135'

    def _clean_balance(self, balance_str):
        # Handle Series case
        if isinstance(balance_str, pd.Series):
            if balance_str.empty:
                return 0.0
            balance_str = balance_str.iloc[0]  # Get first value if Series
        
        if pd.isna(balance_str):
            return 0.0
        if isinstance(balance_str, str):
            return float(balance_str.replace(',', ''))
        return float(balance_str)
    
    def _clean_string(self, input_str):
        # Handle Series case
        if isinstance(input_str, pd.Series):
            if input_str.empty:
                return ""
            input_str = input_str.iloc[0]  # Get first value if Series
        
        if pd.isna(input_str):
            return ""
        return str(input_str).replace("'", "''").strip()

    def _extract_nip(self, rm_string):
        # Handle Series case
        if isinstance(rm_string, pd.Series):
            if rm_string.empty:
                return None
            rm_string = rm_string.iloc[0]  # Get first value if Series
        
        if pd.isna(rm_string) or str(rm_string).strip() == '-':
            return None
        
        rm_string = str(rm_string)
        parts = rm_string.strip().split(' - ', 1)
        if len(parts) != 2:
            return None
            
        nip = parts[0].strip()
        if not nip.isdigit():
            return None
            
        return nip

    def _safe_lower(self, value):
        try:
            # Handle Series case
            if isinstance(value, pd.Series):
                if value.empty:
                    return ""
                value = value.iloc[0]  # Get first value if Series
            
            if pd.isna(value):
                return ""
            return str(value).lower()
        except:
            return str(value)

    def _read_file(self, file_path, file_ext):
        try:
            if file_ext == '.csv':
                return pd.read_csv(
                    file_path, 
                    dtype=str,
                    na_values=['-', ' - ', ''],
                    encoding='utf-8'
                )
            elif file_ext in ['.xlsx', '.xls']:
                return pd.read_excel(
                    file_path, 
                    dtype=str,
                    na_values=['-', ' - ', '']
                )
            else:
                raise ValueError(f"Format file tidak didukung: {file_ext}")
        except UnicodeDecodeError:
            if file_ext == '.csv':
                return pd.read_csv(
                    file_path, 
                    skiprows=4 if file_path == self.source_path else 0, 
                    dtype=str,
                    na_values=['-', ' - ', ''],
                    encoding='latin1'
                )
            raise

    def generate_dml_query(self) -> str:
        try:
            print("--- Model SqlGeneratorModel Mulai Berjalan ---")

            print("[Tahap 1] Membaca dan membersihkan data...")
            
            source_ext = pathlib.Path(self.source_path).suffix.lower()
            baseline_ext = pathlib.Path(self.baseline_path).suffix.lower()
            
            try:
                df_source = self._read_file(self.source_path, source_ext)
                df_baseline = self._read_file(self.baseline_path, baseline_ext)
                
                print(f"   -> Source data: {len(df_source)} baris")
                print(f"   -> Baseline data: {len(df_baseline)} baris")
                
            except Exception as e:
                print(f"Error reading files: {str(e)}")
                raise

            print(f"   -> Source columns: {df_source.columns.tolist()}")
            print(f"   -> Baseline columns: {df_baseline.columns.tolist()}")
            
            # Normalize column names to lowercase
            df_source.columns = df_source.columns.astype(str)
            df_baseline.columns = df_baseline.columns.astype(str)
            
            df_source.columns = [self._safe_lower(col) for col in df_source.columns]
            df_baseline.columns = [self._safe_lower(col) for col in df_baseline.columns]
            
            # Map columns
            col_mapping_source = {
                self._safe_lower(col): col for col in df_source.columns
            }
            col_mapping_baseline = {
                self._safe_lower(col): col for col in df_baseline.columns
            }
            
            # Map source columns
            source_columns = {
                '_col_source_rm': self._col_source_rm,
                '_col_source_rekening': self._col_source_rekening,
                '_col_source_cif': self._col_source_cif,
                '_col_source_nama_klien': self._col_source_nama_klien,
                '_col_source_balance': self._col_source_balance,
                '_col_source_avail_balance': self._col_source_avail_balance,
                '_col_source_currency': self._col_source_currency,
                '_col_source_prod_code': self._col_source_prod_code,
                '_col_date': self._col_date
            }
            
            for attr_name, col_name in source_columns.items():
                lower_col = self._safe_lower(col_name)
                setattr(self, attr_name, col_mapping_source.get(lower_col, col_name))
            
            # Map baseline columns
            baseline_columns = {
                '_col_baseline_pn': self._col_baseline_pn,
                '_col_baseline_nama_rm': self._col_baseline_nama_rm,
                '_col_baseline_rekening': self._col_baseline_rekening
            }
            
            for attr_name, col_name in baseline_columns.items():
                lower_col = self._safe_lower(col_name)
                setattr(self, attr_name, col_mapping_baseline.get(lower_col, col_name))
            
            print(f"   -> Mapped source columns: {self._col_source_rekening}, {self._col_source_rm}, {self._col_source_cif}")
            print(f"   -> Mapped baseline columns: {self._col_baseline_rekening}, {self._col_baseline_pn}, {self._col_baseline_nama_rm}")
            
            # Clean account numbers - remove decimals
            if self._col_source_rekening in df_source.columns:
                df_source[self._col_source_rekening] = df_source[self._col_source_rekening].astype(str).str.split('.').str[0]
            
            if self._col_baseline_rekening in df_baseline.columns:
                df_baseline[self._col_baseline_rekening] = df_baseline[self._col_baseline_rekening].astype(str).str.split('.').str[0]

            # Initialize SQL queries
            all_queries = ["-- Skrip SQL DML Dihasilkan oleh SqlGeneratorModel --\n"]
            all_queries.append("-- Blok 0: Membuat Branch --")
            all_queries.append(f"INSERT INTO branches (name, address, created_at, updated_at) VALUES ('{self.branch_name}', '{self.branch_address}', NOW(), NOW());")
            all_queries.append("SET @branch_id = LAST_INSERT_ID();")

            print("[Tahap 2] Membuat 6 Universal Bankers...")
            
            # Hardcode 6 Universal Bankers
            universal_bankers = [
                {'nip': '00332299', 'name': 'Rino Arya Pradana'},
                {'nip': '00332936', 'name': 'Mutiara Purwaning Rahayu'},
                {'nip': '00350816', 'name': 'Arini Rahmanisa'},
                {'nip': '00364289', 'name': 'Vika Yulia Widiarsih'},
                {'nip': '00351323', 'name': 'Enrico Fadlurahman'},
                {'nip': '00347741', 'name': 'Ollyvia Aulia Rahmah'}
            ]
            
            # Create set of valid NIPs
            valid_nips = set()
            
            all_queries.append("\n-- Blok 1: Membuat Universal Bankers (RM) --")
            for banker in universal_bankers:
                nip = banker['nip']
                name = banker['name']
                valid_nips.add(nip)
                all_queries.append(f"INSERT IGNORE INTO universal_bankers (nip, name, branch_id, created_at, updated_at) VALUES ('{nip}', '{name}', @branch_id, NOW(), NOW());")
                print(f"   -> Added banker: {nip} - {name}")
            
            print(f"   -> Total UB created: {len(universal_bankers)}")
            print(f"   -> Valid NIPs: {valid_nips}")

            print("[Tahap 3] Membuat Account Products dari data source...")
            if self._col_source_prod_code in df_source.columns:
                df_source_clean = df_source.dropna(subset=[self._col_source_prod_code])
                unique_products = df_source_clean[[self._col_source_prod_code]].drop_duplicates()

                all_queries.append("\n-- Blok 2: Membuat Account Products --")
                for _, row in unique_products.iterrows():
                    prod_code = self._clean_string(row[self._col_source_prod_code])
                    if prod_code:
                        all_queries.append(f"INSERT IGNORE INTO account_products (code, name, created_at, updated_at) VALUES ('{prod_code}', 'Produk {prod_code}', NOW(), NOW());")
            else:
                print(f"   -> Warning: Product code column not found in source")
                all_queries.append("-- Warning: Couldn't create account products due to missing column")

            print("[Tahap 4] Memproses data DI dengan validasi baseline...")
            
            # Create baseline lookup for faster checking
            baseline_accounts = set()
            baseline_pn_mapping = {}
            
            if self._col_baseline_rekening in df_baseline.columns and self._col_baseline_pn in df_baseline.columns:
                for _, row in df_baseline.iterrows():
                    try:
                        rekening = self._clean_string(row[self._col_baseline_rekening])
                        pn = self._clean_string(row[self._col_baseline_pn])
                        if rekening and pn:
                            baseline_accounts.add(rekening)
                            baseline_pn_mapping[rekening] = pn
                    except:
                        continue
            
            print(f"   -> Baseline accounts loaded: {len(baseline_accounts)}")
            
            # Process source data
            processed_clients = set()
            processed_accounts = set()
            client_queries = []
            account_queries = []
            transaction_queries = []
            
            all_queries.append("\n-- Blok 3: Membuat Clients (Nasabah) --")
            all_queries.append("\n-- Blok 4: Membuat Accounts (Rekening) --")
            all_queries.append("\n-- Blok 5: Membuat Account Transactions --")
            
            valid_records = 0
            skipped_records = 0
            
            for _, row in df_source.iterrows():
                try:
                    # Check if pn_relationship_officer is not empty
                    if self._col_source_rm in df_source.columns:
                        pn_rm = self._clean_string(row[self._col_source_rm])
                        if not pn_rm or pn_rm == '' or pn_rm == 'nan':
                            skipped_records += 1
                            continue
                    
                    # Get account number
                    if self._col_source_rekening not in df_source.columns:
                        skipped_records += 1
                        continue
                        
                    rekening = self._clean_string(row[self._col_source_rekening])
                    if not rekening:
                        skipped_records += 1
                        continue
                    
                    # Check if account exists in baseline
                    if rekening not in baseline_accounts:
                        skipped_records += 1
                        continue
                    
                    # Get PN from baseline
                    baseline_pn = baseline_pn_mapping.get(rekening, '')
                    if baseline_pn not in valid_nips:
                        print(f"   -> Warning: PN {baseline_pn} for account {rekening} not in valid NIPs")
                        skipped_records += 1
                        continue
                    
                    # Get required fields
                    cif = self._clean_string(row[self._col_source_cif]) if self._col_source_cif in df_source.columns else ''
                    nama_klien = self._clean_string(row[self._col_source_nama_klien]) if self._col_source_nama_klien in df_source.columns else ''
                    prod_code = self._clean_string(row[self._col_source_prod_code]) if self._col_source_prod_code in df_source.columns else ''
                    currency = self._clean_string(row[self._col_source_currency]) if self._col_source_currency in df_source.columns else 'IDR'
                    
                    if not all([cif, nama_klien, prod_code]):
                        skipped_records += 1
                        continue
                    
                    # Create client (if not already processed)
                    if cif not in processed_clients:
                        processed_clients.add(cif)
                        client_queries.append(f"INSERT IGNORE INTO clients (cif, name, status, joined_at, created_at, updated_at) VALUES ('{cif}', '{nama_klien}', 'active', NOW(), NOW(), NOW());")
                    
                    # Create account (if not already processed)
                    if rekening not in processed_accounts:
                        processed_accounts.add(rekening)
                        
                        # Get balances
                        current_balance = 0
                        avail_balance = 0
                        
                        if self._col_source_balance in df_source.columns:
                            current_balance = self._clean_balance(row[self._col_source_balance])
                        if self._col_source_avail_balance in df_source.columns:
                            avail_balance = self._clean_balance(row[self._col_source_avail_balance])
                        
                        account_queries.append(
                            f"INSERT IGNORE INTO accounts (client_id, universal_banker_id, account_product_id, account_number, current_balance, available_balance, currency, status, opened_at, created_at, updated_at) VALUES "
                            f"((SELECT id FROM clients WHERE cif = '{cif}'), "
                            f"(SELECT id FROM universal_bankers WHERE nip = '{baseline_pn}'), "
                            f"(SELECT id FROM account_products WHERE code = '{prod_code}'), "
                            f"'{rekening}', {current_balance}, {avail_balance}, '{currency}', 'active', NOW(), NOW(), NOW());"
                        )
                        
                        # Create transaction
                        transaction_date = 'NOW()'
                        if self._col_date in df_source.columns:
                            periode_value = self._clean_string(row[self._col_date])
                            if periode_value and periode_value != 'nan':
                                try:
                                    # Handle different date formats
                                    date_str = periode_value.strip()
                                    
                                    # Try different date formats
                                    date_formats = [
                                        "%Y-%m-%d", 
                                        "%d/%m/%Y",      
                                        "%Y/%m/%d",     
                                        "%d-%m-%Y",      
                                        "%Y-%m-%d %H:%M:%S",
                                        "%d/%m/%Y %H:%M:%S"
                                    ]
                                    
                                    parsed_date = None
                                    for fmt in date_formats:
                                        try:
                                            parsed_date = datetime.strptime(date_str, fmt)
                                            break
                                        except ValueError:
                                            continue
                                    
                                    if parsed_date:
                                        # Format to MySQL date format (YYYY-MM-DD)
                                        transaction_date = f"'{parsed_date.strftime('%Y-%m-%d')}'"
                                    else:
                                        # If no format matches, try to extract just the date part
                                        if len(date_str) >= 10:
                                            # Extract first 10 characters if it looks like a date
                                            date_part = date_str[:10]
                                            if date_part.count('-') == 2:
                                                # Validate the date format YYYY-MM-DD
                                                try:
                                                    datetime.strptime(date_part, "%Y-%m-%d")
                                                    transaction_date = f"'{date_part}'"
                                                except ValueError:
                                                    transaction_date = 'NOW()'
                                            else:
                                                transaction_date = 'NOW()'
                                        else:
                                            transaction_date = 'NOW()'
                                except Exception as e:
                                    print(f"   -> Warning: Error parsing date '{periode_value}': {e}")
                                    transaction_date = 'NOW()'

                        transaction_queries.append(
                            f"INSERT INTO account_transactions (account_id, balance, date, created_at, updated_at) VALUES "
                            f"((SELECT id FROM accounts WHERE account_number = '{rekening}'), "
                            f"{current_balance}, {transaction_date}, NOW(), NOW());"
                        )
                    
                    valid_records += 1
                    
                except Exception as e:
                    print(f"   -> Warning: Error processing row: {e}")
                    skipped_records += 1
                    continue
            
            print(f"   -> Valid records processed: {valid_records}")
            print(f"   -> Skipped records: {skipped_records}")
            print(f"   -> Unique clients: {len(processed_clients)}")
            print(f"   -> Unique accounts: {len(processed_accounts)}")
            
            # Add queries to main list
            if client_queries:
                all_queries.append("\n".join(client_queries))
            else:
                all_queries.append("-- No client data to process")
            
            if account_queries:
                all_queries.append("\n".join(account_queries))
            else:
                all_queries.append("-- No account data to process")
            
            if transaction_queries:
                all_queries.append("\n".join(transaction_queries))
            else:
                all_queries.append("-- No transaction data to process")

            print(f"[Tahap 5] Menyimpan output ke file: {self.output_sql_file}")
            with open(self.output_sql_file, 'w', encoding='utf-8') as f:
                f.write("\n".join(all_queries))
            
            print(f"\n--- Model Selesai Bekerja ---")
            print(f"SQL file generated: {os.path.abspath(self.output_sql_file)}")
            return self.output_sql_file

        except Exception as e:
            print(f"\n--- MODEL MENGALAMI ERROR ---")
            print(f"Detail Error: {str(e)}")
            import traceback
            traceback.print_exc()
            return f"ERROR: {str(e)}"