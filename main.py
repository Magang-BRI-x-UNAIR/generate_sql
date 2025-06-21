# Nama File: main.py

from model import SqlGeneratorModel

SOURCE_DATA_FILE = 'DUMMY.xlsx'
BASELINE_DATA_FILE = 'DUMMY ALL BASELINE.xlsx'
print("Menginisialisasi SqlGeneratorModel...")
model = SqlGeneratorModel(
    source_filepath=SOURCE_DATA_FILE,
    baseline_filepath=BASELINE_DATA_FILE
)

print("Menjalankan proses pembuatan query DML...")
output_file = model.generate_dml_query()

# 3. Periksa hasilnya
if output_file:
    print(f"\nProses berhasil! Model telah menghasilkan skrip SQL di: {output_file}")
    print("Anda dapat memeriksa file tersebut dan menjalankannya di database Anda.")
else:
    print("\nProses gagal. Silakan periksa log error di atas.")