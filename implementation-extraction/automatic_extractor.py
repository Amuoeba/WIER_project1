# Imports from external libraries
from lxml import html, etree
# Imports from internal libraries
from sequence_alignment import align, Branch
import configs
import utils


class Node:
    def __init__(self, node, pos="start"):
        self.node = node
        self.pos = pos
        if self.pos == "start":
            self.tag = node.tag
        elif self.pos == "end":
            self.tag = f"/{node.tag}"
        else:
            raise ValueError("Wrong pos value. Possibl 'start' or 'end'")

    def __eq__(self, other):
        return self.tag == other.tag

    def __repr__(self):
        return self.tag


def get_printable_formate(row_tags, col_tags):
    printable = []
    for i, n in enumerate(row_tags):
        m = col_tags[i]
        if isinstance(n, Node):
            n = n.node.text
        else:
            n = n
        if isinstance(m, Node):
            m = m.node.text
        else:
            m = m
        printable.append({"site1": n, "site2": m})
    return printable


def node_expansion_check(tag):
    exclude_tags = ["meta", "script", "head", "input"]
    if not isinstance(tag, str):
        return False
    if tag in exclude_tags:
        return False
    return True


def tag_append_check(tag):
    exclude_tags = ["html", "body"]
    if tag in exclude_tags:
        return False
    return True


def get_tag_sequence(tree, tags):
    child_iter = list(etree.ElementChildIterator(tree))
    if node_expansion_check(tree.tag):
        if tag_append_check(tree.tag):
            tags.append(Node(tree, pos="start"))
        if len(child_iter) > 0:
            for child in child_iter:
                tags = get_tag_sequence(child, tags=tags)
        else:
            # if node_expansion_check(tree.tag):
            #     if tag_append_check(tree.tag):
            #         tags.append(Node(tree,pos="end"))
            return tags
    # if node_expansion_check(tree.tag):
    #     if tag_append_check(tree.tag):
    #         tags.append(Node(tree,pos="end"))
    return tags


def find_max_alignment(seq1, seq2):
    if len(seq1) > len(seq2):
        long = seq1
        short = seq2
    else:
        long = seq2
        short = seq1

    offset = 0
    max_same = 0
    max_at_offset = None
    while offset < len(long):
        same = 0
        for i, s_ele in enumerate(short):
            if offset + i < len(long):
                if s_ele == long[offset + i]:
                    same += 1
        if same > max_same:
            max_same = same
            max_at_offset = offset
        offset += 1
    return max_same, max_at_offset, short, long


def clean_tags(opt_aligns: Branch):
    assert (len(opt_aligns.row_seq) == len(opt_aligns.col_seq))

    no_tags = len(opt_aligns.col_seq)
    usable_tags_row = []
    usable_tags_col = []

    filters = [
        lambda x, y: x == y,
        lambda x, y: (x == "-" and y == None) or (x == None and y == "-"),
        lambda x, y: (x == "-" and y == " ") or (x == " " and y == "-"),
        lambda x, y: x in [" ", "-", None] or y in [" ", "-", None]
    ]

    for i in range(no_tags):
        r_tag = opt_aligns.row_seq[i]
        c_tag = opt_aligns.col_seq[i]

        if isinstance(r_tag, Node):
            r_text = r_tag.node.text
        elif isinstance(r_tag, str):
            r_text = r_tag
        else:
            raise TypeError("Wrong type of r_tag")

        if isinstance(c_tag, Node):
            c_text = c_tag.node.text
        elif isinstance(c_tag, str):
            c_text = c_tag
        else:
            raise TypeError("Wrong type of c_tag")

        keep = True
        for f in filters:
            if f(c_text, r_text):
                keep = False

        if keep:
            usable_tags_col.append(c_tag)
            usable_tags_row.append(r_tag)

    return usable_tags_row, usable_tags_col


def generate_wrapper(html1, html2):
    print("Generating wrapper")
    tree1 = html.fromstring(html1)
    tree2 = html.fromstring(html2)

    tags1 = []
    tags2 = []
    tags1 = get_tag_sequence(tree1, tags1)
    tags2 = get_tag_sequence(tree2, tags2)

    # max_same,offset,short,long = find_max_alignment(tags1.copy(),tags2.copy())
    # utils.preety_print_alignment(long,short,offset)
    print(f"Tag lenghts: {len(tags1)},{len(tags2)}")
    alignment_array, opt_aligns = align(tags1, tags2)
    row_tags, col_tags = clean_tags(opt_aligns[0])
    printable = get_printable_formate(row_tags,col_tags)
    return printable


def run():
    # Overstock auto
    html1, html2 = configs.overstock_sites
    print("Running automatic extraction on:")
    print(f"{html1}\n{html2}")
    html1 = utils.read_and_clean_html(html1)
    html2 = utils.read_and_clean_html(html2)

    p = generate_wrapper(html1, html2)
    utils.save_automatic_extraction_results("overstock", p, "automatic")

    # Rtvslo
    html3, html4 = configs.rtvslo_sites
    print("Running automatic extraction on:")
    print(f"{html3}\n{html4}")
    html3 = utils.read_and_clean_html(html3)
    html4 = utils.read_and_clean_html(html4)

    p = generate_wrapper(html3, html4)
    utils.save_automatic_extraction_results("rtvslo", p, "automatic")

    # mobilede
    html5, html6 = configs.mobilede_sites
    print("Running automatic extraction on:")
    print(f"{html5}\n{html6}")
    html5 = utils.read_and_clean_html(html5)
    html6 = utils.read_and_clean_html(html6)

    p = generate_wrapper(html5, html6)
    utils.save_automatic_extraction_results("mobilede",p,"automatic")
