
from pathlib import Path

from assasdb import AssasOdessaNetCDF4Converter

# Set your archive and output path (can be dummy, only report is generated)
current_dir = Path(__file__).parent.parent
input_path = current_dir / "data/archive/LOCA_12P_CL_1300_LIKE.bin"
input_path = str(input_path.resolve())
output_path = current_dir / "data/result/loca_12p_cl_1300_like_test.h5"
output_path = str(output_path.resolve())

# Instantiate the converter, which will generate the report
converter = AssasOdessaNetCDF4Converter(input_path, output_path)

print("Report generated at astec_config/assas_variables_wp2_report.csv")