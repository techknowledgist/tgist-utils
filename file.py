import os, errno


def ensure_path(path, verbose=False):
    """Make sure path exists."""
    try:
        os.makedirs(path)
        if verbose:
            # this only prints if there was no error in makedirs
            print "[ensure_path] creating %s" % (path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

def get_lines(filename, start=0, limit=500):
    """Return a list with n=limit lines from filename, starting from line n=start. This
    function will return less than n=limit files if their were less than n=limit lines
    left in filename, it will return an empty list if ==start is larger than the number of
    lines in the file.""" 
    current_count = start
    fh = open(filename)
    line_number = 0
    while line_number < current_count:
        fh.readline(),
        line_number += 1
    lines_read = 0
    lines = []
    while lines_read < limit:
        line = fh.readline().strip()
        if line == '':
            break
        lines.append(line)
        lines_read += 1
    fh.close()
    return lines

def get_file_paths(source_path):
    """Return a list with all filenames in source_path."""
    file_paths = []
    for (root, dirs, files) in os.walk(source_path):
        for file in files:
            file_paths.append(os.path.join(root, file))
    return file_paths

def create_file(filename, content=None):
    """Create a file with name filename and write content to it if any was given."""
    fh = open(filename, 'w')
    if content is not None:
        fh.write(content)
    fh.close()
