import psycopg2
import csv
import json
import datetime as dt
import os
import importlib



def validation_rule(config, file_name):
    conn = psycopg2.connect(f"dbname={config['dbname']} user={config['user']} password={config['password']}")
    cur = conn.cursor()
    with open(file_name, 'r', encoding='utf8') as f:
        read_file = csv.reader(f)
        next(read_file)
        for item in read_file:
            cur.execute("INSERT INTO validation_rule VALUES (%s, %s, %s, %s);",(item[0],item[1],item[2],item[3]))
    conn.commit()
    conn.close()
        

def validate_data(config, file_name):
    conn = psycopg2.connect(f"dbname={config['dbname']} user={config['user']} password={config['password']}")
    cur = conn.cursor()
    with open(file_name, 'r', encoding='utf8') as f:
        read_file = csv.reader(f)
        header = []
        rule_text = None
        result = []
        execution_time = None

        #base path for fialied sql records
        base_path = "D:/Priyanka/DataEngineering/Personal Project/PAMS"
        for item in read_file:

            if not header:
                header = item
                rule_text_index = item.index("rule_text")
                type = item.index("rule_type")
                continue

            rule_type = item[type]
            rule_text = item[rule_text_index]
            #print(rule_text_index, rule_text)
            execution_time = dt.datetime.now()
            failed_file_name = os.path.join(base_path, item[1] + '.csv')
            #print(execution_time)
            if rule_type == 'SQL':
                cur.execute(rule_text)
                output = cur.fetchall()
                result = [list(res) for res in output]
            #print(result)
                if result:
                    cur.execute("INSERT INTO validation_result (rule_id,rule_text,execution_time,result) VALUES (%s, %s, %s, %s);",(item[0],item[3],execution_time,"Failed!"))
                    with open(failed_file_name, 'w', encoding='utf8') as file:
                        writer = csv.writer(file)
                        writer.writerows(result)
                #file.close()
                else:
                    cur.execute("INSERT INTO validation_result (rule_id,rule_text,execution_time,result) VALUES (%s, %s, %s, %s);",(item[0],item[3],execution_time,"Success!"))

            elif rule_type == 'Python':
                mod = importlib.import_module('validate_name')
                #func = getattr(mod, 'validate_name')
                result = mod.validate_name()
                print(result)
                if result:
                    cur.execute("INSERT INTO validation_result (rule_id,rule_text,execution_time,result) VALUES (%s, %s, %s, %s);",(item[0],item[3],execution_time,"Failed!"))
                    with open(failed_file_name, 'w', encoding='utf8') as file:
                        writer = csv.writer(file)
                        writer.writerow(result)
                else:
                    cur.execute("INSERT INTO validation_result (rule_id,rule_text,execution_time,result) VALUES (%s, %s, %s, %s);",(item[0],item[3],execution_time,"Success!"))


    conn.commit()
    conn.close()





if __name__ == "__main__":
    config = json.loads(open("config.json").read())
    validation_rule(config,"validation_rule.csv")
    validate_data(config, "validation_rule.csv")