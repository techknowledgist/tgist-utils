import os, errno


def ensure_path(path, verbose=False):
    """Make sure path exists."""
    try:
        os.makedirs(path)
        if verbose:
            print "[ensure_path] created %s" % (path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

def get_lines(filename, start=0, limit=500):
    """Return a list with n=limit file specifications from filename, starting
    from line n=start. This function will return less than n=limit files if
    their were less than n=limit lines left in filename, it will return an empty
    list if start is larger than the number of lines in the file."""
    current_count = start
    fh = open(filename)
    line_number = 0
    while line_number < current_count:
        fh.readline(),
        line_number += 1
    lines_read = 0
    fspecs = []
    while lines_read < limit:
        line = fh.readline().strip()
        if line == '':
            break
        fspec = FileSpec(line)
        fspecs.append(fspec)
        lines_read += 1
    fh.close()
    return fspecs

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

def filename_generator(path, filelist):
    """Creates generator on the filelist, yielding the concatenation of the past and a path
    in filelist."""
    fh = open(filelist)
    for line in fh:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        fspec = FileSpec(line)
        yield os.path.join(path, 'files', fspec.target)
    fh.close()


class FileSpec(object):

    """A FileSpec is created from a line from a file that specifies the
    sources. Such a file has two mandatory columns: year and source_file. These
    fill the year and source instance variables in the FileSpec. The target
    instance variable is by default the same as the source, but can be overruled
    if there is a third column in the file. Example input lines:

       1980    /data/patents/xml/us/1980/12.xml   1980/12.xml
       1980    /data/patents/xml/us/1980/13.xml   1980/13.xml
       1980    /data/patents/xml/us/1980/14.xml
       0000    /data/patents/xml/us/1980/15.xml

    """

    def __init__(self, line):
        fields = line.strip().split("\t")
        self.year = fields[0]
        self.source = fields[1]
        self.target = fields[2] if len(fields) > 2 else fields[1]
        self._strip_slashes()

    def __str__(self):
        return "%s\n  %s\n  %s" % (self.year, self.source, self.target)

    def _strip_slashes(self):
        if self.target.startswith(os.sep):
            self.target = self.target[1:]
