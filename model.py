# Nama File: model.py

import pandas as pd
import os
import pathlib

class SqlGeneratorModel:
    """
    Sebuah model 'Expert System' yang tugasnya adalah mengubah data dari file Excel/CSV
    menjadi skrip SQL DML untuk proses INSERT, berdasarkan aturan validasi bisnis.
    """
    
    def __init__(self, source_filepath: str, baseline_filepath: str):
        """
        Inisialisasi model dengan path ke file data.
        """
        self.source_path = source_filepath
        self.baseline_path = baseline_filepath
        self.output_sql_file = 'generated_sql_script.sql'

        # Definisikan nama kolom secara internal
        self._col_source_rm = 'PN Relationship Officer / RM Kredit Menangah'
        self._col_source_rekening = 'account number'
        self._col_source_cif = 'ciff no'
        self._col_source_nama_klien = 'short name'
        self._col_source_balance = 'balance'
        self._col_source_avail_balance = 'available balance'
        self._col_source_currency = 'curr code'
        self._col_source_prod_code = 'prod code'
        self._col_baseline_pn = 'PN'
        self._col_baseline_nama_rm = 'Nama'
        self._col_baseline_rekening = 'Rekening'

    def _clean_balance(self, balance_str):
        """Helper method untuk membersihkan data saldo."""
        if pd.isna(balance_str):
            return 0.0
        if isinstance(balance_str, str):
            return float(balance_str.replace(',', ''))
        return float(balance_str)
    
    def _clean_string(self, input_str):
        """Helper method untuk membersihkan string dari karakter khusus SQL."""
        if pd.isna(input_str):
            return ""
        return str(input_str).replace("'", "''").strip()

    def _extract_nip(self, rm_string):
        """Mengekstrak NIP dari string RM yang memiliki format 'NIP - Nama'."""
        if pd.isna(rm_string) or str(rm_string).strip() == '-':
            return None
        
        # Convert to string first to handle numeric values
        rm_string = str(rm_string)
        parts = rm_string.strip().split(' - ', 1)
        if len(parts) != 2:
            return None
            
        nip = parts[0].strip()
        # Validasi bahwa NIP berupa angka
        if not nip.isdigit():
            return None
            
        return nip

    def _safe_lower(self, value):
        """Safely convert a value to lowercase string."""
        try:
            if pd.isna(value):
                return ""
            return str(value).lower()
        except:
            return str(value)

    def generate_dml_query(self) -> str:
        """
        Metode utama untuk menjalankan model.
        """
        try:
            print("--- Model SqlGeneratorModel Mulai Berjalan ---")

            # 1. Membaca & Membersihkan Data
            print("[Tahap 1] Membaca dan membersihkan data...")
            dtype_spec = {
                self._col_source_rekening: 'str',
                self._col_baseline_rekening: 'str',
                self._col_baseline_pn: 'str'
            }
            
            # Deteksi tipe file berdasarkan ekstensi
            source_ext = pathlib.Path(self.source_path).suffix.lower()
            baseline_ext = pathlib.Path(self.baseline_path).suffix.lower()
            
            # Baca file source dengan menentukan jumlah baris yang dilewati di awal
            try:
                if source_ext == '.csv':
                    df_source = pd.read_csv(
                        self.source_path, 
                        dtype=str,  # Use string for all columns to avoid type issues
                        na_values=['-', ' - ', ''],
                        encoding='utf-8'
                    )
                elif source_ext in ['.xlsx', '.xls']:
                    df_source = pd.read_excel(
                        self.source_path, 
                        dtype=str,
                        na_values=['-', ' - ', '']
                    )
                else:
                    raise ValueError(f"Format file source tidak didukung: {source_ext}")
                
                # Baca file baseline
                if baseline_ext == '.csv':
                    df_baseline = pd.read_csv(
                        self.baseline_path, 
                        dtype=str,  # Use string for all columns to avoid type issues
                        na_values=['-', ' - ', ''],
                        encoding='utf-8'
                    )
                elif baseline_ext in ['.xlsx', '.xls']:
                    df_baseline = pd.read_excel(
                        self.baseline_path, 
                        dtype=str,  # Use string for all columns to avoid type issues
                        na_values=['-', ' - ', '']
                    )
                else:
                    raise ValueError(f"Format file baseline tidak didukung: {baseline_ext}")
                
                print(f"   -> Source data: {len(df_source)} baris")
                print(f"   -> Baseline data: {len(df_baseline)} baris")
                
            except UnicodeDecodeError:
                # Jika encoding UTF-8 gagal, coba dengan encoding lain
                if source_ext == '.csv':
                    df_source = pd.read_csv(
                        self.source_path, 
                        skiprows=4, 
                        dtype=str,
                        na_values=['-', ' - ', ''],
                        encoding='latin1'
                    )
                
                if baseline_ext == '.csv':
                    df_baseline = pd.read_csv(
                        self.baseline_path, 
                        dtype=str,
                        na_values=['-', ' - ', ''],
                        encoding='latin1'
                    )

            # Print column names for debugging
            print(f"   -> Source columns: {df_source.columns.tolist()}")
            print(f"   -> Baseline columns: {df_baseline.columns.tolist()}")
            
            # Convert all column names to strings
            df_source.columns = df_source.columns.astype(str)
            df_baseline.columns = df_baseline.columns.astype(str)
            
            # Bersihkan nama kolom - safely convert to lowercase
            df_source.columns = [self._safe_lower(col) for col in df_source.columns]
            df_baseline.columns = [self._safe_lower(col) for col in df_baseline.columns]
            
            # Standardisasi nama kolom sesuai dengan definisi internal - safely handle mapping
            col_mapping_source = {
                self._safe_lower(col): col for col in df_source.columns
            }
            col_mapping_baseline = {
                self._safe_lower(col): col for col in df_baseline.columns
            }
            
            # Map kolom ke nama sebenarnya di dataframe
            source_rm_lower = self._safe_lower(self._col_source_rm)
            self._col_source_rm = col_mapping_source.get(source_rm_lower, self._col_source_rm)
            
            source_rekening_lower = self._safe_lower(self._col_source_rekening)
            self._col_source_rekening = col_mapping_source.get(source_rekening_lower, self._col_source_rekening)
            
            source_cif_lower = self._safe_lower(self._col_source_cif)
            self._col_source_cif = col_mapping_source.get(source_cif_lower, self._col_source_cif)
            
            source_nama_klien_lower = self._safe_lower(self._col_source_nama_klien)
            self._col_source_nama_klien = col_mapping_source.get(source_nama_klien_lower, self._col_source_nama_klien)
            
            source_balance_lower = self._safe_lower(self._col_source_balance)
            self._col_source_balance = col_mapping_source.get(source_balance_lower, self._col_source_balance)
            
            source_avail_balance_lower = self._safe_lower(self._col_source_avail_balance)
            self._col_source_avail_balance = col_mapping_source.get(source_avail_balance_lower, self._col_source_avail_balance)
            
            source_currency_lower = self._safe_lower(self._col_source_currency)
            self._col_source_currency = col_mapping_source.get(source_currency_lower, self._col_source_currency)
            
            source_prod_code_lower = self._safe_lower(self._col_source_prod_code)
            self._col_source_prod_code = col_mapping_source.get(source_prod_code_lower, self._col_source_prod_code)
            
            baseline_pn_lower = self._safe_lower(self._col_baseline_pn)
            self._col_baseline_pn = col_mapping_baseline.get(baseline_pn_lower, self._col_baseline_pn)
            
            baseline_nama_rm_lower = self._safe_lower(self._col_baseline_nama_rm)
            self._col_baseline_nama_rm = col_mapping_baseline.get(baseline_nama_rm_lower, self._col_baseline_nama_rm)
            
            baseline_rekening_lower = self._safe_lower(self._col_baseline_rekening)
            self._col_baseline_rekening = col_mapping_baseline.get(baseline_rekening_lower, self._col_baseline_rekening)
            
            # Print the mapped column names for debugging
            print(f"   -> Mapped source columns: {self._col_source_rekening}, {self._col_source_rm}, {self._col_source_cif}")
            print(f"   -> Mapped baseline columns: {self._col_baseline_rekening}, {self._col_baseline_pn}, {self._col_baseline_nama_rm}")
            
            # Check if the mapped columns exist in the dataframes
            if self._col_source_rekening not in df_source.columns:
                print(f"   -> Warning: Column '{self._col_source_rekening}' not found in source data")
                print(f"   -> Available columns: {list(df_source.columns)}")
            
            if self._col_baseline_rekening not in df_baseline.columns:
                print(f"   -> Warning: Column '{self._col_baseline_rekening}' not found in baseline data")
                print(f"   -> Available columns: {list(df_baseline.columns)}")
            
            # Bersihkan nomor rekening (hilangkan bagian desimal jika ada)
            if self._col_source_rekening in df_source.columns:
                df_source[self._col_source_rekening] = df_source[self._col_source_rekening].astype(str).str.split('.').str[0]
            
            if self._col_baseline_rekening in df_baseline.columns:
                df_baseline[self._col_baseline_rekening] = df_baseline[self._col_baseline_rekening].astype(str).str.split('.').str[0]

            # Inisialisasi list untuk menyimpan query SQL
            all_queries = ["-- Skrip SQL DML Dihasilkan oleh SqlGeneratorModel --\n"]

            # 2. Logika untuk Universal Bankers (dari baseline)
            print("[Tahap 2] Menjalankan aturan untuk 'universal_bankers'...")
            # Check if columns exist before using them
            if self._col_baseline_pn in df_baseline.columns and self._col_baseline_nama_rm in df_baseline.columns:
                df_baseline.dropna(subset=[self._col_baseline_pn, self._col_baseline_nama_rm], inplace=True)
                unique_bankers = df_baseline[[self._col_baseline_pn, self._col_baseline_nama_rm]].drop_duplicates()
                
                # Extract valid PN values from baseline for later validation
                valid_pn_values = set(df_baseline[self._col_baseline_pn].astype(str).str.strip())
                print(f"   -> Valid PN values from baseline: {valid_pn_values}")
                
                all_queries.append("-- Blok 1: Membuat Universal Bankers (RM) --")
                for _, row in unique_bankers.iterrows():
                    pn = self._clean_string(row[self._col_baseline_pn])
                    nama_rm = self._clean_string(row[self._col_baseline_nama_rm])
                    all_queries.append(f"INSERT IGNORE INTO universal_bankers (nip, name, created_at, updated_at) VALUES ('{pn}', '{nama_rm}', NOW(), NOW());")
            else:
                print(f"   -> Warning: Couldn't process universal bankers due to missing columns")
                all_queries.append("-- Warning: Couldn't process universal bankers due to missing columns")
                # Create an empty set if no valid PN values are found
                valid_pn_values = set()
            
            # 3. Logika untuk Account Products (dari source)
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

            # 4. Preprocessing data source - FILTER ROWS WITHOUT RM OR WITH INVALID RM
            print("[Tahap 4] Membersihkan dan memvalidasi data...")
            # Filter out rows where RM is just a dash
            if self._col_source_rm in df_source.columns:
                # Remove rows where RM is just a dash or empty
                df_source = df_source[~((df_source[self._col_source_rm] == '-') | 
                                       (df_source[self._col_source_rm].isna()) | 
                                       (df_source[self._col_source_rm].astype(str).str.strip() == ''))]
                
                # Extract NIP from RM field
                df_source['nip_cleaned'] = df_source[self._col_source_rm].apply(self._extract_nip)
                
                # Filter out rows with invalid NIP format
                df_source_with_rm = df_source.dropna(subset=['nip_cleaned'])
                
                # IMPORTANT: Filter out rows where NIP is not in the baseline valid PNs
                df_source_with_rm = df_source_with_rm[df_source_with_rm['nip_cleaned'].isin(valid_pn_values)]
                
                print(f"   -> Data dengan RM valid yang ada di baseline: {len(df_source_with_rm)} dari {len(df_source)}")
                
                # 5. Validasi dengan baseline - MATCH BY REKENING
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

                    # 6. Buat client dan account dari data tervalidasi
                    all_queries.append("\n-- Blok 3: Membuat Clients (Nasabah) --")
                    
                    if not validated_df.empty:
                        # Check if required columns exist
                        required_cols = [self._col_source_cif, self._col_source_nama_klien]
                        if all(col in validated_df.columns for col in required_cols):
                            # Ambil unique clients
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
                        
                        # Buat account queries
                        all_queries.append("\n-- Blok 4: Membuat Accounts (Rekening) --")
                        
                        required_acc_cols = [self._col_source_cif, 'nip_cleaned', self._col_source_prod_code, 
                                            self._col_source_rekening, self._col_source_currency]
                        
                        if all(col in validated_df.columns for col in required_acc_cols):
                            # Create a set to track unique account numbers to avoid duplicates
                            processed_accounts = set()
                            account_queries = []
                            
                            for _, row in validated_df.iterrows():
                                rekening = self._clean_string(row[self._col_source_rekening])
                                
                                # Skip if this account has already been processed
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
                else:
                    all_queries.append("-- Tidak bisa memvalidasi data karena kolom rekening tidak ditemukan")
                    print(f"   -> Warning: Couldn't validate accounts due to missing rekening columns")
            else:
                all_queries.append("-- Tidak bisa memproses data karena kolom RM tidak ditemukan")
                print(f"   -> Warning: RM column '{self._col_source_rm}' not found")

            # 7. Menyimpan Hasil
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