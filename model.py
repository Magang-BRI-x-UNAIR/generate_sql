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
        if pd.isna(balance_str):
            return 0.0
        if isinstance(balance_str, str):
            return float(balance_str.replace(',', ''))
        return float(balance_str)
    
    def _clean_string(self, input_str):
        if pd.isna(input_str):
            return ""
        return str(input_str).replace("'", "''").strip()

    def _extract_nip(self, rm_string):
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
            
            df_source.columns = df_source.columns.astype(str)
            df_baseline.columns = df_baseline.columns.astype(str)
            
            df_source.columns = [self._safe_lower(col) for col in df_source.columns]
            df_baseline.columns = [self._safe_lower(col) for col in df_baseline.columns]
            
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
            
            # Check for missing columns
            missing_source_cols = [col for col, attr in source_columns.items() 
                                if getattr(self, col) not in df_source.columns]
            if missing_source_cols:
                print(f"   -> Warning: Missing source columns: {missing_source_cols}")
                
            missing_baseline_cols = [col for col, attr in baseline_columns.items() 
                                  if getattr(self, col) not in df_baseline.columns]
            if missing_baseline_cols:
                print(f"   -> Warning: Missing baseline columns: {missing_baseline_cols}")
            
            # Clean account numbers
            if self._col_source_rekening in df_source.columns:
                df_source[self._col_source_rekening] = df_source[self._col_source_rekening].astype(str).str.split('.').str[0]
            
            if self._col_baseline_rekening in df_baseline.columns:
                df_baseline[self._col_baseline_rekening] = df_baseline[self._col_baseline_rekening].astype(str).str.split('.').str[0]

            all_queries = ["-- Skrip SQL DML Dihasilkan oleh SqlGeneratorModel --\n"]
            all_queries.append("-- Blok 0: Membuat Branch --")
            all_queries.append(f"INSERT INTO branches (name, address, created_at, updated_at) VALUES ('{self.branch_name}', '{self.branch_address}', NOW(), NOW());")
            all_queries.append("SET @branch_id = LAST_INSERT_ID();")

            print("[Tahap 2] Menjalankan aturan untuk 'universal_bankers'...")
            if self._col_baseline_pn in df_baseline.columns and self._col_baseline_nama_rm in df_baseline.columns:
                df_baseline.dropna(subset=[self._col_baseline_pn, self._col_baseline_nama_rm], inplace=True)
                unique_bankers = df_baseline[[self._col_baseline_pn, self._col_baseline_nama_rm]].drop_duplicates()
                
                valid_pn_values = set(df_baseline[self._col_baseline_pn].astype(str).str.strip())
                print(f"   -> Valid PN values from baseline: {valid_pn_values}")
                
                all_queries.append("\n-- Blok 1: Membuat Universal Bankers (RM) --")
                for _, row in unique_bankers.iterrows():
                    pn = self._clean_string(row[self._col_baseline_pn])
                    nama_rm = self._clean_string(row[self._col_baseline_nama_rm])
                    all_queries.append(f"INSERT IGNORE INTO universal_bankers (nip, name, branch_id, created_at, updated_at) VALUES ('{pn}', '{nama_rm}', @branch_id, NOW(), NOW());")
            else:
                print(f"   -> Warning: Couldn't process universal bankers due to missing columns")
                all_queries.append("-- Warning: Couldn't process universal bankers due to missing columns")
                valid_pn_values = set()
            
            print("[Tahap 3] Menjalankan aturan untuk 'account_products'...")
            if self._col_source_prod_code in df_source.columns:
                df_source.dropna(subset=[self._col_source_prod_code], inplace=True)
                unique_products = df_source[[self._col_source_prod_code]].drop_duplicates()

                all_queries.append("\n-- Blok 2: Membuat Account Products --")
                for _, row in unique_products.iterrows():
                    prod_code = self._clean_string(row[self._col_source_prod_code])
                    all_queries.append(f"INSERT IGNORE INTO account_products (code, name, created_at, updated_at) VALUES ('{prod_code}', 'Produk {prod_code}', NOW(), NOW());")
            else:
                print(f"   -> Warning: Couldn't process account products due to missing column: {self._col_source_prod_code}")
                all_queries.append("-- Warning: Couldn't process account products due to missing product code column")

            print("[Tahap 4] Membersihkan dan memvalidasi data...")
            processed_accounts = set()
            if self._col_source_rm in df_source.columns:
                df_source = df_source[~((df_source[self._col_source_rm] == '-') | 
                                      (df_source[self._col_source_rm].isna()) | 
                                      (df_source[self._col_source_rm].astype(str).str.strip() == ''))]
                
                df_source['nip_cleaned'] = df_source[self._col_source_rm].apply(self._extract_nip)
                
                df_source_with_rm = df_source.dropna(subset=['nip_cleaned'])
                
                df_source_with_rm = df_source_with_rm[df_source_with_rm['nip_cleaned'].isin(valid_pn_values)]
                
                print(f"   -> Data dengan RM valid yang ada di baseline: {len(df_source_with_rm)} dari {len(df_source)}")
                
                print("[Tahap 5] Menjalankan validasi dengan baseline...")
                if self._col_source_rekening in df_source_with_rm.columns and self._col_baseline_rekening in df_baseline.columns:
                    validated_df = pd.merge(
                        df_source_with_rm, 
                        df_baseline, 
                        left_on=self._col_source_rekening,
                        right_on=self._col_baseline_rekening,
                        how='inner',
                        suffixes=('', '_baseline')
                    )
                    
                    print(f"   -> Model menemukan {len(validated_df)} data valid dari {len(df_source_with_rm)} setelah dicocokkan dengan baseline")

                    all_queries.append("\n-- Blok 3: Membuat Clients (Nasabah) --")
                    
                    if not validated_df.empty:
                        required_cols = [self._col_source_cif, self._col_source_nama_klien]
                        if all(col in validated_df.columns for col in required_cols):
                            unique_clients = validated_df[[self._col_source_cif, self._col_source_nama_klien]].drop_duplicates()
                            
                            client_queries = []
                            for _, row in unique_clients.iterrows():
                                cif = self._clean_string(row[self._col_source_cif])
                                client_name = self._clean_string(row[self._col_source_nama_klien])
                                client_queries.append(f"INSERT IGNORE INTO clients (cif, name, status, joined_at, created_at, updated_at) VALUES ('{cif}', '{client_name}', 'active', NOW(), NOW(), NOW());")
                            
                            all_queries.append("\n".join(client_queries))
                        else:
                            all_queries.append("-- Tidak bisa membuat clients karena kolom yang diperlukan tidak ditemukan")
                            print(f"   -> Warning: Couldn't create clients due to missing columns")
                        
                        all_queries.append("\n-- Blok 4: Membuat Accounts (Rekening) --")
                        
                        required_acc_cols = [self._col_source_cif, 'nip_cleaned', self._col_source_prod_code, 
                                             self._col_source_rekening, self._col_source_currency]
                        
                        if all(col in validated_df.columns for col in required_acc_cols):
                            processed_accounts = set()
                            account_queries = []
                            
                            for _, row in validated_df.iterrows():
                                rekening = self._clean_string(row[self._col_source_rekening])
                                
                                if rekening in processed_accounts:
                                    continue
                                    
                                processed_accounts.add(rekening)
                                
                                cif = self._clean_string(row[self._col_source_cif])
                                nip = self._clean_string(row['nip_cleaned'])
                                prod_code = self._clean_string(row[self._col_source_prod_code])
                                currency = self._clean_string(row[self._col_source_currency])
                                
                                try:
                                    if self._col_source_balance in validated_df.columns:
                                        current_balance = self._clean_balance(row[self._col_source_balance])
                                    else:
                                        current_balance = 0
                                        
                                    if self._col_source_avail_balance in validated_df.columns:
                                        avail_balance = self._clean_balance(row[self._col_source_avail_balance])
                                    else:
                                        avail_balance = 0
                                except Exception as e:
                                    print(f"   -> Kesalahan memproses saldo untuk rekening {rekening}: {e}")
                                    current_balance = 0
                                    avail_balance = 0
                                
                                account_queries.append(
                                    f"INSERT IGNORE INTO accounts (client_id, universal_banker_id, account_product_id, account_number, current_balance, available_balance, currency, status, opened_at, created_at, updated_at) VALUES "
                                    f"((SELECT id FROM clients WHERE cif = '{cif}'), "
                                    f"((SELECT id FROM universal_bankers WHERE nip = '{nip}')), "
                                    f"((SELECT id FROM account_products WHERE code = '{prod_code}')), "
                                    f"'{rekening}', {current_balance}, {avail_balance}, '{currency}', 'active', NOW(), NOW(), NOW());"
                                )
                            
                            all_queries.append("\n".join(account_queries))
                        else:
                            all_queries.append("-- Tidak bisa membuat accounts karena kolom yang diperlukan tidak ditemukan")
                            missing_cols = [col for col in required_acc_cols if col not in validated_df.columns]
                            print(f"   -> Warning: Couldn't create accounts due to missing columns: {missing_cols}")
                    else:
                        all_queries.append("-- Tidak ada data valid untuk pembuatan Clients")
                        all_queries.append("\n-- Tidak ada data valid untuk pembuatan Accounts")
                        print("   -> No validated data found")
                    
                    # Fixed Account Transactions section
                    all_queries.append("\n-- Blok 5: Membuat Account Transactions --")
                    transaction_queries = []
                    
                    # Check if date column exists
                    has_date = self._col_date in validated_df.columns
                    default_date = datetime.now().strftime("%Y-%m-%d")
                    
                    for _, row in validated_df.iterrows():
                        rekening = self._clean_string(row[self._col_source_rekening])
                        
                        # Skip if account wasn't processed earlier
                        if rekening not in processed_accounts:
                            continue
                            
                        try:
                            if has_date:
                                date_str = self._clean_string(row[self._col_date])
                                if not date_str:
                                    date = default_date
                                else:
                                    try:
                                        for fmt in ["%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y"]:
                                            try:
                                                date = datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
                                                break
                                            except ValueError:
                                                continue
                                        else: 
                                            date = default_date
                                    except Exception:
                                        date = default_date
                            else:
                                date = default_date
                                
                            if self._col_source_balance in validated_df.columns:
                                current_balance = self._clean_balance(row[self._col_source_balance])
                            else:
                                current_balance = 0
                        except Exception as e:
                            print(f"   -> Kesalahan memproses saldo untuk transaksi rekening {rekening}: {e}")
                            current_balance = 0
                            date = default_date
                        
                        transaction_queries.append(
                            f"INSERT INTO account_transactions (account_id, balance, date, created_at, updated_at) VALUES "
                            f"((SELECT id FROM accounts WHERE account_number = '{rekening}'), "
                            f"{current_balance}, '{date}', NOW(), NOW());"
                        )
                    
                    if transaction_queries:
                        all_queries.append("\n".join(transaction_queries))
                    else:
                        all_queries.append("-- Tidak ada data untuk pembuatan Account Transactions")
   
                else:
                    all_queries.append("-- Tidak bisa memvalidasi data karena kolom rekening tidak ditemukan")
                    print(f"   -> Warning: Couldn't validate accounts due to missing rekening columns")
            else:
                all_queries.append("-- Tidak bisa memproses data karena kolom RM tidak ditemukan")
                print(f"   -> Warning: RM column '{self._col_source_rm}' not found")

            print(f"[Tahap 7] Menyimpan output model ke file: {self.output_sql_file}")
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