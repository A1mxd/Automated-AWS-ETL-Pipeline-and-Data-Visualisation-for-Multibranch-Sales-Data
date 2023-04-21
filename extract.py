import csv

def open_csv_as_dict_list(file):
    dict_list = []
    try:
        with open(file, "r") as f:
            reader = csv.reader(f)
            for line in reader:
                entry = {
                    "date_time": line[0],
                    "location" : line[1],
                    "customer_name" : line[2],
                    "order" : line[3],
                    "total_price" : line[4],
                    "payment_method" : line[5],
                    "card_number" : line[6]
                    }
                dict_list.append(entry)
        return dict_list
    except:
        print(f"failed to open {file}")


if __name__ == '__main__':
    orders = open_csv_as_dict_list("chesterfield_25-08-2021_09-00-00.csv")
    
    for i,order in enumerate(orders):
            if i < 5:
                print(f"{i}:\tdate_time:\t\t{order['date_time']}")
                print(f"\tlocation:\t{order['location']}")
                print(f"\tcustomer_name:\t{order['customer_name']}")
                print(f"\torder:\t{order['order']}")
                print(f"\ttotal_price:\t{order['total_price']}")
                print(f"\tpayment_method:\t{order['payment_method']}")
                print(f"\tcard_number:\t\t{order['card_number']}\n")


