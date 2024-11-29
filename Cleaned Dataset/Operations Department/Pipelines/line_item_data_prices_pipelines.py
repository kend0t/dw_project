import pandas as pd

prices1 = pd.read_csv('../../../Raw Dataset/Operations Department/line_item_data_prices1.csv')
prices2 = pd.read_csv('../../../Raw Dataset/Operations Department/line_item_data_prices2.csv')
prices3 = pd.read_parquet('../../../Raw Dataset/Operations Department/line_item_data_prices3.parquet')

prices = [prices1, prices2, prices3]
allprice = pd.concat(prices)

# transform quantity column
allprice['quantity'] = allprice['quantity'].str.extract(r'(\d+)').astype(int)

#drop Unnamed: 0 cols
allprice = allprice.drop('Unnamed: 0', axis=1)

#make csv
allprice.to_csv("cleaned_line_item_data_prices.csv", index=False)