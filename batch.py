"""

Various utilities for ontology creation and pattern matcher.

"""

import os, time, shutil


def read_stages(target_path, language):
    """Read the counts in target_path/language/ALL_STAGES.txt."""
    stages = {}
    for line in open(os.path.join(target_path, language, 'ALL_STAGES.txt')):
        if not line.strip():
            continue
        (stage, count) = line.strip().split("\t")
        stages[stage] = int(count)
    return stages

def update_stages(target_path, language, stage, limit):
    """Updates the counts in target_path/language/ALL_STAGES.txt. This includes rereading
    the file because during processing on one machine another machine could have done some
    other processing and have updated the fiel, we do not want to lose those updates. This
    could potentially go wrong when two separate processes terminate at the same time, a
    rather unlikely occurrence."""
    stages = read_stages(target_path, language)
    stages.setdefault(stage, 0)
    stages[stage] += limit
    write_stages(target_path, language, stages)
    
def write_stages(target_path, language, stages):
    """Write stages counts to target_path/language/ALL_STAGES.txt."""
    stages_file = os.path.join(target_path, language, 'ALL_STAGES.txt')
    backup_file = os.path.join(target_path, language,
                               "ALL_STAGES.%s.txt" % time.strftime("%Y%m%d-%H%M%S"))
    shutil.copyfile(stages_file, backup_file)
    fh = open(stages_file, 'w')
    for stage, count in stages.items():
        fh.write("%s\t%d\n" % (stage, count))
    fh.close()

def files_to_process(target_path, language, stages, stage, limit):
    """Return a list of <year, filename> pairs from ALL_FILES.txt, using the stages in
    ALL_STAGES.txt and the limit given."""
    current_count = stages.setdefault(stage, 0)
    files = open(os.path.join(target_path, language, 'ALL_FILES.txt'))
    line_number = 0
    while line_number < current_count:
        files.readline(),
        line_number += 1
    files_read = 0
    fnames = []
    while files_read < limit:
        fname = files.readline().strip()
        basename = os.path.basename(fname)
        dirname = os.path.dirname(fname)
        year = os.path.split(dirname)[1]
        fnames.append((year, basename))
        files_read += 1
    return fnames


def files_to_process2(target_path, language, stages, stage, limit):
    """Return a list of <year, filename> pairs from ALL_FILES.txt, using the stages in
    ALL_STAGES.txt and the limit given."""

    # This is now more complicated. It includes (i) getting the data directory you are
    # working on (eg en/tag), (ii) using the config-pipeline to find what subdirectory to
    # use, (iii) reading the local stages file in there (which might now be called
    # something like processing-dribble or what not)
    
    current_count = stages.setdefault(stage, 0)
    files = open(os.path.join(target_path, language, 'config', 'files.txt'))
    line_number = 0
    while line_number < current_count:
        files.readline(),
        line_number += 1
    files_read = 0
    fnames = []
    while files_read < limit:
        fname = files.readline().strip()
        basename = os.path.basename(fname)
        dirname = os.path.dirname(fname)
        year = os.path.split(dirname)[1]
        fnames.append((year, basename))
        files_read += 1
    return fnames
