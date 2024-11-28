## copy code sa notebook/pipeline nyo ayusin nyo nalang ung line nung para sa fie directory
import pandas as pd;
pd.set_option('display.max_colwidth', None)  # No truncation for column values
pd.set_option('display.expand_frame_repr', False)

# PRODUCT ID LOOKUP TABLE
product_list = pd.read_excel('../../../Raw Dataset/Business Department/product_list.xlsx')
product_list = product_list.sort_values(by='product_id')
product_list

products_lookup = product_list[['product_id','product_name','price']].copy()
products_lookup['new_id'] = ['PRODUCT{:05d}'.format(i) for i in range(1,len(product_list)+1)]
products_lookup


# USER ID LOOKUP TABLE
user_data = pd.read_json('../../../Raw Dataset/Customer Management Department/user_data.json')
#sort by creation date
user_data = user_data.sort_values(by='creation_date')
users_lookup = user_data[['user_id','creation_date','name']].copy()
users_lookup['new_id'] = ['USER{:05d}'.format(i) for i in range(1,len(user_data)+1)]
users_lookup


