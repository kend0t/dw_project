import pandas as pd
import os

base_dir = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(base_dir, '../../../Raw Dataset/Operations Department/line_item_data_prices1.csv')
prices1 = pd.read_csv(file_path)
file_path = os.path.join(base_dir, '../../../Raw Dataset/Operations Department/line_item_data_prices2.csv')
prices2 = pd.read_csv(file_path)
file_path = os.path.join(base_dir,'../../../Raw Dataset/Operations Department/line_item_data_prices3.parquet')
prices3 = pd.read_parquet(file_path)

prices = [prices1, prices2, prices3]
allprice = pd.concat(prices)

# transform quantity column
allprice['quantity'] = allprice['quantity'].str.extract(r'(\d+)').astype(int)

#drop Unnamed: 0 cols
allprice = allprice.drop('Unnamed: 0', axis=1)

#make csv
file_path = os.path.join(base_dir, '../New Files/cleaned_line_item_data_prices.csv')
allprice.to_csv(file_path, index=False)