import sys
import argparse
from random import randint

MAIN_FOLDER = "ClueWeb12_"


def generate_collection_spec(clueweb_root, num_files, out_path):
    with open(out_path, "w") as file_out:
        for i in xrange(0, num_files):
            first_num = randint(0, 19)
            second_num = randint(0, 12)
            third_num = randint(0, 99)
            first_folder = MAIN_FOLDER + str(first_num).zfill(2)
            second_folder = str(first_num).zfill(2) + str(second_num).zfill(2) + "wb"
            filename = second_folder + "-" + str(third_num).zfill(2) + ".warc.gz"
            warc_path = clueweb_root.rstrip("/") + "/" + first_folder + "/" + second_folder + "/" + filename
            print warc_path
            file_out.write(warc_path + "\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate ClueWeb12 collection.spec for Terrier.')
    parser.add_argument("clueweb_root", metavar="clueweb_root", help="Path of the ClueWeb12 folder")
    # noinspection PyTypeChecker
    parser.add_argument("num_files", metavar="num_files", type=int, help="Number of .warc.gz files to index")
    parser.add_argument("out_path", metavar="out_path", help="Path for the generated collection.spec file")
    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)
    args = parser.parse_args()
    generate_collection_spec(args.clueweb_root, args.num_files, args.out_path)
