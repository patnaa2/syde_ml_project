import csv
import sys

'''
    We are doing some edge case labelling, so we should have a sanity check
    on the new csv versus the old one, before we actually merge the data,
    in case I screw something up badly
'''

def read_data_from_csv(csv_f, skip_first=True):
    ret = []

    with open(csv_f) as csvfile:
        reader = csv.reader(csvfile)
        count = 0
        for row in reader:
            if count == 0 and skip_first:
                count += 1
                continue
            count += 1
            ret.append(row)

    return ret

def main():
    orig_f = "labelled_data.csv"
    new_f = "updated_data.csv"

    orig = read_data_from_csv(orig_f)
    new = read_data_from_csv(new_f)

    # Make sure the lengths of the data are the same first
    if len(orig) != len(new):
        print "THE LENGTH OF THE DATA SEEMS TO HAVE CHANGED... NOT POSSIBLE"
        print len(orig)
        print len(new)
        sys.exit(1)

    # Check we should have gained more labels ie. did we add more hits
    old_labels = len([x for x in orig if x[-1] == "1"])
    new_labels = len([x for x in new if x[-1] == "1"])

    print old_labels
    print new_labels

    if old_labels > new_labels:
        print "WE SEEM TO HAVE LOST HITS SINCE THE UPDATE.. NOT POSSIBLE"
        print old_labels
        print new_labels
        sys.exit(1)

    print "Labelled %s as hits from old data" %(new_labels - old_labels)
    print "Updates to old data seem to be reasonable, be careful before merging"

if __name__ == "__main__":
    main()
