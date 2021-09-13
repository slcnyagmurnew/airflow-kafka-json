import time
from json import dump

product1 = {
    "barcode": 2893438909,
    "amount": 89
}

product2 = {
    "barcode": 2893438969,
    "amount": 67
}
our_list = [product1, product2]
with open('./data/logs/log-' + str(time.strftime("%Y%m%d_%H%M")) + '.json', 'w') as outfile:
    dump(our_list, outfile)
    outfile.close()
