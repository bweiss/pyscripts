"""EDI claim parser - Parses EDI claim files and splits them into multiple files based on claim type

Author: Brian Weiss <brianmweiss@gmail.com> - 2022

Requirements:
    - Python 3.6 or later
    - MILC (https://milc.clueboard.co/)

The plan:

The data should come in the form of a single line file with segments delimited by '~' and elements within those
segments delimited by '*' (though this is configurable).

Each data set should begin with two header segments named 'ISA' and 'GS'. These should be preserved and written to
the output files. Following the header segments there should be any number of sets of claim data with the start of
the data signified by the 'ST' segment and the end of the data by the 'SE' segment. Within this claim set data there
will be a 'BHT' segment that contains the element or field that we'll pull the claim type from. Finally, the entire
data set should be followed by a couple of footer segments named 'GE' and 'IEA'. Similar to the header, these should
be preserved and written to the resulting output files. Finally, within the 'GE' segment is an element containing the
total record count for the data set. When writing our output files, we need to re-write this value to represent
the new claim count after splitting by type.

Output files should be written to the specified output directory in the same format as the original data file
but with claims data split into separate files by type. Filenames should be in the following format.

<input_file_prefix>_<claim_type>.<input_file_suffix>
"""

import csv
import os
import sys
from enum import Enum, auto, Flag
from milc import set_metadata
set_metadata(name='edi_parser', version='0.1.0', author='Brian Weiss')
from milc import cli
import milc.subcommand.config

SEGMENT_DELIMITER = '~'
ELEMENT_DELIMITER = '*'
HEADER_SEGMENTS = ['ISA', 'GS']
FOOTER_SEGMENTS = ['GE', 'IEA']
FOOTER_CLAIM_COUNT_SEGMENT = 'GE'
CLAIM_START_SEGMENT = 'ST'
CLAIM_END_SEGMENT = 'SE'
CLAIM_TYPE_SEGMENT = 'BHT'
CLAIM_TYPE_ELEMENT_NUM = 3
CLAIM_COUNT_ELEMENT_NUM = 1

header = []
footer = []
claims = []
claim_counts = {}


class ClaimType(Enum):
    CE = auto()
    CF = auto()

    def __repr__(self):
        return "%s" % self.name


class ErrorCode(Enum):
    ERR_MISSING_SUBCOMMAND = 1
    ERR_MISSING_ARG = 2
    ERR_CANT_READ_FILE = 3
    ERR_CANT_WRITE_FILE = 4
    ERR_HEADER_NOT_FOUND = 5
    ERR_FOOTER_NOT_FOUND = 6
    ERR_CLAIM_DATA_NOT_FOUND = 7
    ERR_UNKNOWN = 256


class Claim:
    def __init__(self, claim_type, data):
        self.claim_type = claim_type
        self.data = data


def init():
    for claim_type in ClaimType:
        claim_counts.__setitem__(claim_type, 0)


@cli.entrypoint('EDI claim parser')
def main(cli):
    cli.print_usage()
    handle_error(ErrorCode.ERR_MISSING_SUBCOMMAND, 'No subcommand specified!')


@cli.argument('-i', '--input_file', help='The EDI claim file to verify')
@cli.subcommand('Verify EDI claim file.')
def verify(cli):
    """Verify an EDI claim file.
    """
    if not cli.config.verify.input_file:
        cli.print_usage()
        handle_error(ErrorCode.ERR_MISSING_ARG, 'No input file specified!')

    parse_input_file(cli.config.verify.input_file)
    print_summary_report(cli.config.verify.input_file)


@cli.argument('-o', '--output_dir', help='The directory to place the output files in')
@cli.argument('-i', '--input_file', help='The EDI claim file to split')
@cli.subcommand('Split claims into separate files.')
def split(cli):
    """Split an EDI claim file into multiple files by claim type
    """
    if not cli.config.split.input_file:
        cli.print_usage()
        handle_error(ErrorCode.ERR_MISSING_ARG, 'No input file specified!')

    if not cli.config.split.output_dir:
        cli.print_usage()
        handle_error(ErrorCode.ERR_MISSING_ARG, 'No output directory specified!')

    parse_input_file(cli.config.split.input_file)

    if not header:
        handle_error(ErrorCode.ERR_HEADER_NOT_FOUND, 'Header not found')

    if not footer:
        handle_error(ErrorCode.ERR_FOOTER_NOT_FOUND, 'Footer not found')

    if not claims:
        handle_error(ErrorCode.ERR_CLAIM_DATA_NOT_FOUND, 'Claim data not found')

    cli.log.debug('Attempting to write output files')
    input_filename = os.path.basename(cli.config.split.input_file)
    first_period = input_filename.index('.')
    prefix = input_filename[0:first_period]
    suffix = input_filename[first_period:len(input_filename)]
    for claim_type in ClaimType:
        if claim_counts[claim_type]:
            write_output_file(cli.config.split.output_dir, claim_type, prefix, suffix)

    cli.echo("Wrote claim files to %s, counts: %s", cli.config.split.output_dir, claim_counts)


def print_summary_report(input_file):
    cli.echo("Input file: %s", input_file)
    cli.echo("Header segments: %d", len(header))
    cli.echo("Footer segments: %s", len(footer))
    cli.echo("Claims: %s", claim_counts)


def parse_input_file(filename):
    """Split an EDI claim file into multiple files by claim type.
    """
    try:
        fh = open(filename, "r")
    except (FileNotFoundError, PermissionError) as ex:
        handle_error(ErrorCode.ERR_CANT_READ_FILE, ex)

    cur_claim_data = []
    cur_claim_type = None
    reader = csv.reader(fh, delimiter=SEGMENT_DELIMITER)

    for segments in reader:
        for segment in segments:
            # this extra check helps avoid inserting an empty segment at the end of the claim data
            if segment:
                # header segments
                if segment.startswith(tuple(HEADER_SEGMENTS)):
                    header.append(segment)

                # footer segments
                elif segment.startswith(tuple(FOOTER_SEGMENTS)):
                    footer.append(segment)

                else:

                    # start of a new claim
                    if segment.startswith(CLAIM_START_SEGMENT):
                        cur_claim_data = [segment]

                    # end of claim
                    elif segment.startswith(CLAIM_END_SEGMENT):
                        cur_claim_data.append(segment)
                        claim = Claim(cur_claim_type, cur_claim_data)
                        claims.append(claim)
                        claim_counts[cur_claim_type] = claim_counts[cur_claim_type] + 1

                    # all other claim segments
                    else:
                        cur_claim_data.append(segment)

                        # this segment has our type value
                        if segment.startswith(CLAIM_TYPE_SEGMENT):
                            elements = segment.split(ELEMENT_DELIMITER)
                            type_element = elements[CLAIM_TYPE_ELEMENT_NUM]
                            cur_claim_type = ClaimType[type_element[:2]]

    cli.log.debug("Finished parsing file: %s, type_counts: %s", filename, claim_counts)


def write_output_file(output_dir, claim_type, prefix, suffix):
    """Write claim data for a particular claim type to the output directory.
    """
    if claim_counts[claim_type]:
        filename = output_dir + os.path.sep + prefix + "_" + claim_type.name + suffix

        try:
            fh = open(filename, "w")
        except (FileNotFoundError, PermissionError) as ex:
            handle_error(ErrorCode.ERR_CANT_WRITE_FILE, ex)

        # header
        fh.write(SEGMENT_DELIMITER.join(header))

        # claim data
        for claim_item in claims:
            if claim_item.claim_type == claim_type:
                fh.write(SEGMENT_DELIMITER)
                fh.write(SEGMENT_DELIMITER.join(claim_item.data))

        # footer
        for segment in footer:
            fh.write(SEGMENT_DELIMITER)
            if segment.startswith(FOOTER_CLAIM_COUNT_SEGMENT):
                elements = segment.split(ELEMENT_DELIMITER)
                elements[CLAIM_COUNT_ELEMENT_NUM] = str(claim_counts[claim_type])
                segment = ELEMENT_DELIMITER.join(elements)
            fh.write(segment)

        fh.close()

        cli.log.debug("Finished writing claim file: %s (claim count: %d)", filename, claim_counts[claim_type])
    else:
        cli.log.debug("No claim data for type %s, no file written", claim_type)


def handle_error(error_code, message, *args):
    """Handle an error, logging it and then exiting with the supplied exit code.
    """
    cli.log.error("%s: %s %s", error_code.name, message, args)
    sys.exit(error_code.value)


if __name__ == '__main__':
    try:
        init()
        cli()
    except Exception as error:
        handle_error(ErrorCode.ERR_UNKNOWN, error)
