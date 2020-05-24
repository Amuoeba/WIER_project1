# Imports from external libraries
import numpy as np


# Imports from internal libraries
class Branch:
    def __init__(self, row_seq, col_seq, cur_row, cur_col):
        self.row_seq = row_seq
        self.col_seq = col_seq
        self.cur_row = cur_row
        self.cur_col = cur_col

    @staticmethod
    def preety_print_aligned(seq1, seq2):
        s1 = []
        s2 = []
        for i, x1 in enumerate(seq1):
            x2 = seq2[i]
            if x1 == "-":
                x1 = x1 * len(x2)
            elif x2 == "-":
                x2 = x2 * len(x1)
            elif len(x1) > len(x2):
                diff = len(x1) - len(x2)
                x2 += " " * diff
            elif len(x1) < len(x2):
                diff = len(x2) - len(x1)
                x1 += " " * diff

            s1.append(x1)
            s2.append(x2)
        print(s1)
        print(s2)

    def __repr__(self):
        # self.preety_print_aligned(self.row_seq, self.col_seq)
        return f'RowSeq: {self.row_seq} ColSeq: {self.col_seq} Index:{self.cur_row, self.cur_col}'


def backtrack(alignment_arr, cols, rows, limit=None):
    alignments = []
    branchings = []

    match = 1
    missmatch = -1
    indel = -1

    row = alignment_arr.shape[0] - 1
    col = alignment_arr.shape[1] - 1

    # Init branches
    ismatch = rows[row] == cols[col]

    curval = alignment_arr[row][col]
    opts = {"top": alignment_arr[row - 1][col],
            "left": alignment_arr[row][col - 1],
            "diag": alignment_arr[row - 1][col - 1]
            }
    for o in opts:
        if o == "top":
            if curval == opts[o] + indel:
                branchings.append(Branch([rows[row]], ["-"], row - 1, col))
        if o == "left":
            if curval == opts[o] + indel:
                branchings.append(Branch(["-"], [cols[col]], row, col - 1))
        if o == "diag":
            ismatch = rows[row] == cols[col]
            if ismatch:
                if curval == opts[o] + match:
                    branchings.append(Branch([rows[row]], [cols[col]], row - 1, col - 1))
            else:
                if curval == opts[o] + missmatch:
                    branchings.append(Branch([rows[row]], [cols[col]], row - 1, col - 1))

    while len(branchings) > 0:
        branch = branchings.pop(0)
        cur_row_seq = branch.row_seq
        cur_col_seq = branch.col_seq
        row = branch.cur_row
        col = branch.cur_col

        if row == 0 and col == 0:
            alignments.append(branch)
            if limit and len(alignments) >= limit:
                return alignments
        else:
            curval = alignment_arr[row][col]
            opts = {"top": alignment_arr[row - 1][col],
                    "left": alignment_arr[row][col - 1],
                    "diag": alignment_arr[row - 1][col - 1]
                    }
            for o in opts:
                if o == "top":
                    if curval == opts[o] + indel:
                        r = cur_row_seq.copy()
                        c = cur_col_seq.copy()
                        r.insert(0, rows[row])
                        c.insert(0, "-")
                        branchings.insert(0, Branch(r, c, row - 1, col))
                if o == "left":
                    if curval == opts[o] + indel:
                        r = cur_row_seq.copy()
                        c = cur_col_seq.copy()
                        r.insert(0, "-")
                        c.insert(0, cols[col])
                        branchings.insert(0, Branch(r, c, row, col - 1))
                if o == "diag":
                    ismatch = rows[row] == cols[col]
                    if ismatch:
                        if curval == opts[o] + match:
                            r = cur_row_seq.copy()
                            c = cur_col_seq.copy()
                            r.insert(0, rows[row])
                            c.insert(0, cols[col])
                            branchings.insert(0, Branch(r, c, row - 1, col - 1))
                    else:
                        if curval == opts[o] + missmatch:
                            r = cur_row_seq.copy()
                            c = cur_col_seq.copy()
                            r.insert(0, rows[row])
                            c.insert(0, cols[col])
                            branchings.insert(0, Branch(r, c, row - 1, col - 1))
    return alignments


def align(a, b):
    match = 1
    missmatch = -1
    indel = -1

    cols = ["#"] + [x for x in a]
    rows = ["#"] + [x for x in b]
    arr = np.zeros((len(rows), len(cols)))

    for c in range(len(cols)):
        for r in range(len(rows)):
            print(f"\rProcessing: {r} | {c}",end="")
            if c == 0 and r == 0:
                arr[0][0] = 0
            elif c == 0:
                arr[r][c] = arr[r - 1][c] + indel
            elif r == 0:
                arr[r][c] = arr[r][c - 1] + indel
            else:
                top = arr[r - 1][c] + indel
                left = arr[r][c - 1] + indel
                if cols[c] == rows[r]:
                    diag = arr[r - 1][c - 1] + match
                else:
                    diag = arr[r - 1][c - 1] + missmatch

                opts = np.array([top, left, diag])
                max_opt = np.argmax(opts)
                max_val = opts[max_opt]
                arr[r][c] = max_val
    print(arr)
    opt_alignments = backtrack(arr, cols, rows, limit=1)

    return arr, opt_alignments



if __name__ == "__main__":
    print("Testing align code")
    a = "GCATGCU"
    b = "GATTACA"
    res_arr, opt_alignment = align(a, b)
    print(res_arr)
    for i in opt_alignment:
        print(i)

    a2 = ["ab", "abc", "ab"]
    b2 = ["ab", "ab", "abc"]
    res_arr1, opt_alignment1 = align(a2, b2)
    print(res_arr1)
    for i in opt_alignment1:
        print(i)


