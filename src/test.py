import csv
naicsRef_ = {}
with open("%s%s" % (r"00_resources/","naicsCodes.csv"), 'r', newline='') as csvfile:
    csv_reader = csv.reader(csvfile)
    for row in csv_reader:
        naicsRef_[str(row[0])] = str(row[1])

print(naicsRef_)