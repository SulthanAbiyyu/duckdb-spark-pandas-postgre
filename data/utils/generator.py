from faker import Faker
from tqdm import tqdm
from datetime import datetime
import random
import pandas as pd
import numpy as np

NUM_ROW = 500_000
fake = Faker(["id_ID"])

class Choices:
    @staticmethod
    def get_product_names():
        return [
            "shampoo",
            "laptop",
            "jam_tangan",
            "kemeja",
            "kaca_mata",
            "kursi",
            "meja"
        ]
    
    @staticmethod
    def product_map():
        return {
        "shampoo":"kecantikan",
        "laptop":"teknologi",
        "jam_tangan":"aksesoris",
        "kemeja":"fashion",
        "kaca_mata":"aksesoris",
        "kursi":"perabotan",
        "meja":"perabotan"
        } 
        
    @staticmethod
    def product_id_map():
        return {
        "shampoo":1,
        "laptop":2,
        "jam_tangan":3,
        "kemeja":4,
        "kaca_mata":5,
        "kursi":6,
        "meja":7
        } 
        
    @staticmethod
    def get_product_category(product):
        return Choices.product_map()[product]

    @staticmethod
    def get_product_id(product):
        return Choices.product_id_map()[product]
    
def _generate():
    sale_id=[]
    customer_id=[]
    customer_name=[]
    customer_address=[]
    product_id=[]
    product_name=[]
    product_category=[]
    amount=[]
    sale_date =[]
    
    known_customers = {}

    for i in tqdm(range(NUM_ROW)):
        sale_id.append(random.randint(100000, 999999))
        
        if i == 0:
            c_id = random.randint(100000, 999999)
            c_name = fake.name()
            c_addr = fake.address()
            
            known_customers[c_id] = [c_name, c_addr]
        else:
            c_id = random.randint(100000, 999999)
            c_name = known_customers.get(c_id, fake.name())
            c_addr = known_customers.get(c_id, fake.address())
            
            if type(c_name) == list:
                c_name = c_name[0]
            
            if type(c_addr) == list:
                c_addr = c_addr[1]
            
            known_customers[c_id] = [c_name, c_addr]
            
            
        
        customer_id.append(c_id)
        customer_name.append(c_name)
        customer_address.append(c_addr)
        
        product = str(np.random.choice(
                a = Choices.get_product_names(),
                size = 1
            )[0])
        product_cat = Choices.get_product_category(product)
        product_identifier = Choices.get_product_id(product)
        product_name.append(product)
        product_category.append(product_cat)
        product_id.append(product_identifier)
        amount.append(random.randint(1,10))
        sale_date.append(fake.date_between(start_date=datetime(2022,1,1)))
        
    return sale_id, customer_id, customer_name, customer_address, product_id, product_name, product_category, amount, sale_date

def _construct_csv(column_names=["sale_id", "customer_id", "customer_name", "customer_address", "product_id", "product_name", "product_category", "amount", "sale_date"]):
    df = pd.DataFrame()
    datas = _generate()
    for col_name, data in tqdm(zip(column_names, datas)):
        df[col_name] = data
    return df

if __name__ == "__main__":
    OUTPUT_PATH = "./data/source_2.csv"
    # df = _construct_csv(["s_id", "c_id", "c_name", "c_address", "p_id", "p_name", "p_category", "amount", "sale_date"])
    df = _construct_csv()
    df = df.sort_values(by="sale_date", ascending=True)
    
    # print(df.head())
    print("writing..")
    df.to_csv(OUTPUT_PATH, sep=",")
    
    