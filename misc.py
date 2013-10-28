"""

Miscellaneous utilities.

"""


def findall(haystack, needle, idx=0):
    """Finds the beginning offset of all occurrences of needle in haystack and
    returns a list of those offsets."""
    offsets = []
    while idx > -1:
        idx = haystack.find(needle, idx)
        if idx > -1:
            offsets.append(idx)
            idx += 1
    return offsets



class BinSorter(object):

    def __init__(self):
        self.bins = {
            0: '0.00-0.05', 1: '0.05-0.10',
            2: '0.10-0.15', 3: '0.15-0.20',
            4: '0.20-0.25', 5: '0.25-0.30',
            6: '0.30-0.35', 7: '0.35-0.40',
            8: '0.40-0.45', 9: '0.45-0.50',
            10: '0.50-0.55', 11: '0.55-0.60',
            12: '0.60-0.65', 13: '0.65-0.70',
            14: '0.70-0.75', 15: '0.75-0.80',
            16: '0.80-0.85', 17: '0.85-0.90',
            18: '0.90-0.95', 19: '0.95-1.00' }
        self.number_of_bins = len(self.bins)

    def find_bin(self, score):
        bin_for_score = int(score * self.number_of_bins)
        if bin_for_score == self.number_of_bins:
            bin_for_score = bin_for_score - 1
        print "%2d  %s  %f  %s" % (bin_for_score, self.bins[bin_for_score], score, score)

    def test(self):
        scores = [0.00000, 1.3375632947763128E-4, 1.3375632947763128E-7,
                  0.143, 0.34,
                  0.3999999999, 0.4, 0.4000000,
                  0.999999, 1.00000, 1]
        for score in scores:
            self.find_bin(score)



if __name__ == '__main__':
    BinSorter().test()
    exit()
