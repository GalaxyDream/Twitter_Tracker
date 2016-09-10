import csv
def csv_reader(input_path):
    with open(input_path,'rb') as csvfile:
        reader = csv.reader(csvfile)
        column = [row[0] for row in reader]
    print (column)
if __name__=="__main__":

    csv_reader('./HPV_relevant_anoucement.csv')