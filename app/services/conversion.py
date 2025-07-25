import scipy.io
import zarr
import numpy as np
import os

def mat_file_conversion(mat_file_path, output_dir):
    mat_data = scipy.io.loadmat(mat_file_path, squeeze_me=True, struct_as_record=False)
    base_name = os.path.splitext(os.path.basename(mat_file_path))[0]
    zarr_folder = os.path.join(output_dir, base_name + '.zarr')

    os.makedirs(output_dir, exist_ok=True)
    root = zarr.open_group(zarr_folder, mode='w')

    convert_to_zarr(mat_data, root)
    return zarr_folder, base_name

def save_struct(group, struct_obj):
    for field in struct_obj._fieldnames:
        val = getattr(struct_obj, field)

        if isinstance(val, np.ndarray):
            if val.dtype == 'object':
                for i, item in enumerate(val):
                    if hasattr(item, '_fieldnames'):
                        subgrp = group.create_group(f"{field}_{i}")
                        save_struct(subgrp, item)
                    else:
                        group.create_dataset(field, data=val, shape=val.shape, overwrite=True)
            else:
                group.create_dataset(field, data=val, shape=val.shape, overwrite=True)
        elif hasattr(val, '_fieldnames'):
            subgrp = group.create_group(field)
            save_struct(subgrp, val)
        elif isinstance(val, (int, float, str, np.number)):
            group.attrs[field] = val

def convert_to_zarr(mat_data, root):
    for varname, val in mat_data.items():
        if varname.startswith('__'):
            continue
        if hasattr(val, '_fieldnames'):
            group = root.create_group(varname)
            save_struct(group, val)
        elif isinstance(val, (int, float, str, np.number)):
            root.attrs[varname] = val