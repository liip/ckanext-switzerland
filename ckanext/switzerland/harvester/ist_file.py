import csv
import os


def ist_file_filter(harvester_obj, config):
    """
    Filter the BPUIC column. This is the station id.
    The filter removes stations from outsite switzerland (those not starting with 85)
    and trims the id to 7 digits (from the front), this strips away additional station data which are appended
    to the station id, e.g. platform.
    """
    temp_file = harvester_obj['file'] + '.tmp'
    with open(temp_file, 'w') as fout, open(harvester_obj['file']) as fin:
        writer = csv.writer(fout, delimiter=';')
        reader = csv.reader(fin, delimiter=';')

        heading = next(reader)

        column_index = None
        for i, column in enumerate(heading):
            if column.strip() == 'BPUIC':
                column_index = i
                break

        if not column_index:
            raise Exception('File {} is not a valid Ist-File, missing column BPUIC'.format(harvester_obj['file']))

        writer.writerow(heading)

        for line in reader:
            bpuic = line[column_index]
            if not bpuic.startswith('85'):  # filter out non-swiss stations
                continue

            line[column_index] = bpuic[:7]  #
            writer.writerow(line)

    os.remove(harvester_obj['file'])
    os.rename(temp_file, harvester_obj['file'])

    return harvester_obj
