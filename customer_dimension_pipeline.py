import pandas as pd
import os

# Load the cleaned customer management department files
base_dir = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(base_dir, 'Cleaned Dataset/Customer Management Department/New Files/cleaned_user_data.csv')
user_data = pd.read_csv(file_path)

file_path = os.path.join(base_dir, 'Cleaned Dataset/Customer Management Department/New Files/cleaned_user_job.csv')
user_job = pd.read_csv(file_path)

file_path = os.path.join(base_dir, 'Cleaned Dataset/Customer Management Department/New Files/cleaned_user_credit_card.csv')
user_credit_card= pd.read_csv(file_path)

customer_dimension = user_data.merge(
    user_job,
    left_on=['user_id','name'],
    right_on=['user_id','name'],
    how='left'
)
customer_dimension = customer_dimension.merge(
    user_credit_card,
    left_on=['user_id','name'],
    right_on=['user_id','name'],
    how='left'
)

customer_dimension = customer_dimension.drop(columns=['old_id'])

print(customer_dimension)






