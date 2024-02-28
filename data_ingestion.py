import psycopg2
import csv
import json


def cleaned_price(value):
    num = []
    decimal_found = False
    for ch in value:
        if ch in [',', ' ']:
            continue
        if not ch.isdigit():
            if not num:
                continue                    
            if ch == '.' and not decimal_found:
                decimal_found = True
            else:
                break                
        num += ch
            
    num = "".join(num)
    number = float(num) if num else 0
    return number


def country_code_to_currency_code():
    country_code_to_currency_code = {}
    with open("country-code-to-currency-code-mapping.csv", 'r') as f:
        read_file = csv.reader(f)
        next(read_file)
        for line in read_file:
            country_code = line[1]
            code = line[3]
            country_code_to_currency_code[country_code] = code
    return country_code_to_currency_code


def currency_exchange():
    currency_exchange = {}
    with open("exchange rate.csv", 'r') as f:
        read_file = csv.reader(f)
        next(read_file)
        for line in read_file:
            exchange_rate = line[2]
            code = line[1]
            currency_exchange[code] = exchange_rate
    return currency_exchange


def clean_price(config, file_name):
    conn = psycopg2.connect(f"dbname={config['dbname']} user={config['user']} password={config['password']}")
    cur = conn.cursor()
    cc = country_code_to_currency_code()
    ce = currency_exchange()
    converted_price = 0
    with open(file_name, "r", encoding="utf8") as f:
        opened_file = csv.reader(f)        
        headers = []
        price_index = None
        for item in opened_file:
            
            if not headers:
                headers = item 
                price_index = item.index("price")
                continue

            price = item[price_index]            
            number = cleaned_price(price)
            country_code = item[3]
            
            if country_code.upper() in cc:
                code = cc[country_code.upper()]
                if code in ce:
                    converted_price = number * float(ce[code])
                    #print(float(final_price))
                    cur.execute("INSERT INTO products VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);",(item[0],item[1],item[2],converted_price,item[3],item[4],item[5],item[6],item[7]))
    
    conn.commit()
    conn.close()


if __name__ == "__main__":
    config = json.loads(open("config.json").read())
    clean_price(config, "apple_products_list.csv")