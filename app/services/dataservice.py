from scipy.io import loadmat
import numpy as np
import pandas as pd
from scipy.signal import savgol_filter

# Load the .mat file
filename = "D:/UoS/COMP6200/backend/project/app/files/MScDataDummy.mat"
mat = loadmat(filename)
sdata = mat["SData"]

# Function to extract and smooth physiological data (e.g., Bp)
def extract_structure_safe(colname: str) -> pd.DataFrame:
    bp_data = sdata[colname][0, 0][0, 0].flatten()
    bp_data = bp_data[np.isfinite(bp_data)]
    bp_data = bp_data[(bp_data >= 30) & (bp_data <= 300)]

    if len(bp_data) >= 0:
        smoothed = savgol_filter(bp_data, window_length=201, polyorder=5)
    else:
        smoothed = bp_data

    return pd.DataFrame({colname: smoothed})

# Function to get time values
def get_time() -> pd.DataFrame:
    time_data = sdata['Time'][0, 0].flatten()
    time_data = time_data[np.isfinite(time_data)]
    time_data = pd.DataFrame({'Time': time_data})
    return time_data
# === Example usage ===
def extract_columns(colname: str):
    bp_df = extract_structure_safe(colname)
    return bp_df

def extract_col_names(prefix=""):
    obj = sdata[0]
    structure = {}
    if hasattr(obj, 'dtype') and obj.dtype.names:  # MATLAB struct
        for name in obj.dtype.names:
            try:
                val = obj[name]
                while isinstance(val, np.ndarray) and val.size == 1:
                    val = val[0]
                full_name = f"{prefix}.{name}" if prefix else name
                if hasattr(val, 'dtype') and val.dtype.names:
                    structure.update(extract_structure_safe(val, prefix=full_name))
                else:
                    structure[full_name] = val.shape if hasattr(val, 'shape') else type(val)
            except Exception as e:
                structure[full_name] = f"Error: {e}"
    else:
        structure[prefix] = obj.shape if hasattr(obj, 'shape') else type(obj)
    return structure