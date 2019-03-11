import pandas as pd
import json
from pathlib import Path

config_path = Path.cwd() / 'config'
cat_matrix_path = config_path / 'Category_Matrix_20180718_simple.xlsx'
json_path = config_path / 'main_categories.json'

df = pd.read_excel(cat_matrix_path, index_col='Three_Char_Aux_Cat')
json_dict = df.apply(lambda x: x[x == 1].index.tolist(), axis=0).to_dict()
with open(json_path, 'w') as f:
    json.dump(json_dict, f, indent=4)