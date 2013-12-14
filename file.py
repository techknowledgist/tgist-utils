import os, errno, stat, subprocess, gzip, codecs


def read_only(filename):
    """Set permissions on filename to read only."""
    os.chmod(filename, stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH)

def open_input_file(filename):
    """First checks whether there is a gzipped version of filename, if so, it
    returns a StreamReader instance. Otherwise, filename is a regular
    uncompressed file and a file object is returned."""
    # TODO: generalize this over reading and writing (or create two methods)
    #print "[file.py in open_input_file] filename: %s" % filename
    if os.path.exists(filename + '.gz'):
        #print "file.py: in if, filename: %s" % (filename + '.gz')
        gzipfile = gzip.open(filename + '.gz', 'rb')
        reader = codecs.getreader('utf-8')
        return reader(gzipfile)
    elif os.path.exists(filename):
        # fallback case, possibly needed for older runs
        return codecs.open(filename, encoding='utf-8')
    else: 
        print "[file.py open_input_file]file does not exist: %s" % filename
        
def open_output_file(fname, compress=True):
    """Return a StreamWriter instance on the gzip file object if compress is
    True, otherwise return a file object."""
    if compress:
        gzipfile = gzip.open(fname + '.gz', 'wb')
        writer = codecs.getwriter('utf-8')
        return writer(gzipfile)
    else:
        return codecs.open(fname, 'w', encoding='utf-8')

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
    """Creates generator on the filelist, yielding the concatenation of the path
    and a path in filelist."""
    fh = open(filelist)
    for line in fh:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        fspec = FileSpec(line)
        yield os.path.join(path, 'files', fspec.target)
    fh.close()

def compress(*fnames):
    """Compress all filenames fname in *fnames using gzip. Checks first if the
    filename exists, it it doesn't it will assume that a file fname.gz already
    exists and it will not attempt to compress."""
    for fname in fnames:
        if not os.path.exists(fname + '.gz'):
            subprocess.call(['gzip', fname])

def uncompress(*fnames):
    """Uncompress all files fname in *fnames using gunzip. The fname argument
    does not include the .gz extension, it is added by this function. If a file
    fname already exists, the function will not attempt to uncompress."""
    for fname in fnames:
        if not os.path.exists(fname):
            subprocess.call(['gunzip', fname + '.gz'])

def get_year_and_docid(path):
    """Get the year and the document name from the file path. This is a tad
    dangerous since it relies on a particular directory structure, but it works
    with how the patent directories are set up, where each patent is directly
    inside a year directory. If there is no year directory, the year returned
    will be 9999."""
    year = os.path.basename(os.path.dirname(path))
    doc_id = os.path.basename(path)
    if not (len(year) == 4 and year.isdigit()):
        year = '9999'
    return (year, doc_id)

def get_year(path):
    """Get the year from the path name. Returns 9999 if no obvious year was
    found. See the comment in get_year_and_docid()."""
    year = os.path.basename(os.path.dirname(path))
    if not (len(year) == 4 and year.isdigit()):
        year = '9999'
    return year


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

    FileSpec can also be created from a line with just one field, in that case
    the year and source are set to None and the target to the only field. This
    is typically used for files that simply list filenames for testing or
    training.
    """

    def __init__(self, line):
        fields = line.strip().split("\t")
        if len(fields) > 1:
            self.year = fields[0]
            self.source = fields[1]
            self.target = fields[2] if len(fields) > 2 else fields[1]
        else:
            self.year = None
            self.source = None
            self.target = fields[0]
        self._strip_slashes()

    def __str__(self):
        return "%s\n  %s\n  %s" % (self.year, self.source, self.target)

    def _strip_slashes(self):
        if self.target.startswith(os.sep):
            self.target = self.target[1:]



class FileData(object):

    """An instance contains a terms dictionary with term information from the
    phr_feats file, amended with a context taken from the tags file. Each term
    is an instance of Term and contains a list of TermInstances. Each
    TermInstance provides access to the features and the context of the
    instance."""

    def __init__(self, tag_file, feat_file, verbose=False):
        self.verbose = verbose
        self.tag_file = tag_file
        self.feat_file = feat_file
        self._init_collect_lines_from_tag_file()
        self._init_collect_term_info_from_phrfeats_file()
        self._init_amend_term_info()

    def __str__(self):
        return "<FileData\n   %s\n   %s>" % (self.tag_file, self.feat_file)

    def get_title(self):
        for section, line in self.tags:
            if section == 'FH_TITLE:':
                return ' '.join(line)
        return ''

    def get_abstract(self):
        abstract = []
        for section, line in self.tags:
            if section == 'FH_ABSTRACT:':
                abstract.append(' '.join(line))
        return ' '.join(abstract)

    def get_term(self, term):
        """Return the Term instance for term or None if term is not in the
        dictionary."""
        return self.terms.get(term)

    def get_terms(self):
        """Returns the list of terms (just the strings) of all terms in the
        dictionary."""
        return self.terms.keys()

    def get_term_instances_dictionary(self):
        """Returns a dictionary indexed on document offsets (sentence
        numbers). The values are lists of TermInstances."""
        terms = {}
        for t in self.get_terms():
            term = self.get_term(t)
            for inst in term.term_instances:
                terms.setdefault(inst.doc_loc, []).append(inst)
        return terms

    def _init_collect_lines_from_tag_file(self):
        self.tags = []
        with open_input_file(self.tag_file) as fh:
            section = None
            for line in fh:
                if line.startswith('FH_'):
                    section = line.strip()
                else:
                    tokens = line.rstrip().split(' ')
                    tokens = [t.rpartition('_')[0] for t in tokens]
                    self.tags.append([section, tokens])

    def _init_collect_term_info_from_phrfeats_file(self):
        self.terms = {}
        with open_input_file(self.feat_file) as fh:
            section = None
            for line in fh:
                (id, year, term, feats) = parse_feats_line(line)
                locfeats = dict((k,v) for (k,v) in feats.items()
                                if k.endswith('_loc'))
                self.terms.setdefault(term, []).append([id, year, feats, locfeats])

    def _init_amend_term_info(self):
        """Replaces the term_data lists in the self.terms dictionary with
        instances of the Term class. Also adds context information from the tag
        data."""
        if self.verbose:
            print "\nGathering term info from tags and feats in %s..." \
                % os.path.basename(tag_file)
        for term in self.terms:
            t = Term(term)
            for term_data in self.terms[term]:
                term_instance = TermInstance(term, term_data)
                context = self.tags[term_instance.doc_loc]
                term_instance.add_context(context)
                t.add_instance(term_instance)
            self.terms[term] = t

    def print_terms(self, limit=5):
        print "\n%s\n" % self
        count = 0
        for term in self.terms:
            #print type(term), term
            count += 1
            if count > limit: break
            self.terms[term].pp()
            print


class Term(object):

    """A Term is basically a container for a list of TermInstances. The
    instances are accessible in the instance variable term_instances."""

    def __init__(self, term):
        self.term = term
        self.term_instances = []

    def __str__(self):
        term_string = "<Term '%s'>" % self.term
        return term_string.encode("UTF-8")

    def add_instance(self, instance):
        self.term_instances.append(instance)

    def pp(self):
        print self
        for instance in self.term_instances:
            print "  %s" % instance


class TermInstance(object):

    """A TermInstance provides access to (i) all features for the term, (ii) the
    context of the term, and (iii) the position of the term in the document and
    the context (as a list of tokens)."""

    def __init__(self, term, term_data):
        self.term = term
        self.id = term_data[0]
        self.doc = term_data[0].rstrip('01234567890')[:-5]
        self.year = term_data[1]
        self.feats = term_data[2]
        doc_loc = self.feats.get('doc_loc')
        sent_loc = self.feats.get('sent_loc')
        if doc_loc.startswith('sent'):
            doc_loc = doc_loc[4:]
        tok1, tok2 = sent_loc.split('-')
        self.doc_loc = int(doc_loc)
        self.sent_loc = (int(tok1), int(tok2))
        self.tok1 = int(tok1)
        self.tok2 = int(tok2)

    def __str__(self):
        string = "<TermInstance %s %s %d-%d '%s'>" \
            % (self.id, self.doc_loc, self.tok1, self.tok2, self.context_token())
        return string.encode("UTF-8")

    def __cmp__(self, other):
        comparison1 = cmp(self.doc_loc, other.doc_loc)
        if comparison1 != 0:
            return comparison1
        return cmp(self.tok1, other.tok1)

    def add_context(self, context):
        self.context = context

    def context_section(self):
        return self.context[0]

    def context_all(self):
        return "%s [%s] %s" % (self.context_left(), self.context_token(), self.context_right())

    def context_token(self):
        return ' '.join(self.context[1][self.tok1:self.tok2])

    def context_left(self):
        return ' '.join(self.context[1][:self.tok1])

    def context_right(self):
        return ' '.join(self.context[1][self.tok2:])



def parse_feats_line(line):
    """Parse a line from a phr_feats file and return a tuple with id year,
    term and features."""
    # TODO: this is similar to the function parse_phr_feats_line() in
    # step6_index.py, with which it should be merged
    (id, year, term, feats) = line.strip().split("\t", 3)
    feats = feats.split("\t")
    feats = dict((k,v) for (k,v) in [f.split('=', 1) for f in feats])
    return (id, year, term, feats)
