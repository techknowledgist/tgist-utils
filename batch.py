"""

Various utilities for ontology creation.

"""

import os, time, shutil

from ontology.utils.file import ensure_path, get_lines, create_file
from ontology.utils.git import get_git_commit


def read_pipeline_config(pipeline_file):
    """Parse pipeline_file and return a list with pipeline objects. Each pipeline object
    is a pair of stage name and options dictionary."""
    pipeline = []
    for line in open(pipeline_file):
        if line.strip() == '':
            continue
        if line.strip()[0] == '#':
            continue
        split_line = line.strip().split()
        step = split_line[0]
        settings = {}
        for feat_val in split_line[1:]:
            if '=' in feat_val:
                feat, val = feat_val.strip().split('=')
            else:
                feat = feat_val.strip()
                val = 'True'
            settings[feat] = val
        pipeline.append((step, settings))
    return pipeline

def pipeline_component_as_string(pipeline_slice):
    """Returns a string representation of a pipeline slice."""
    elements = []
    for element in pipeline_slice:
        elements.append(element[0] + " " +
                        " ".join(["%s=%s" % (k,v) for k,v in element[1].items()]))
    return "\n".join(elements).strip() + "\n"

def get_datasets(config, stage, input_name):
    """Return a list with DataSet objects consisting of all datasets defined for a data
    type."""
    dirname = os.path.join(config.target_path, 'data', input_name)
    datasets1 = [ds for ds in os.listdir(dirname) if ds.isdigit()]
    datasets2 = [DataSet(stage, input_name, config, ds) for ds in datasets1]
    return datasets2

def show_datasets(rconfig, data_types):
    """Print all datasets in the data directory."""
    for dataset_type in data_types:
        print "\n===", dataset_type, "===\n"
        path = os.path.join(rconfig.target_path, 'data', dataset_type)
        datasets1 = [ds for ds in os.listdir(path) if ds.isdigit()]
        datasets2 = [DataSet(None, dataset_type, rconfig, ds) for ds in datasets1]
        for ds in datasets2:
            print ds
            for e in ds.pipeline_trace:
                print "   ", e[0], e[1]
            print "   ", ds.pipeline_head[0], ds.pipeline_head[1]

def show_pipelines(rconfig):
    path = os.path.join(rconfig.target_path, 'config')
    pipeline_files = [f for f in os.listdir(path) if f.startswith('pipeline')]
    for pipeline_file in sorted(pipeline_files):
        if pipeline_file[-1] == '~':
            continue
        print "\n[%s]" % pipeline_file
        for line in open(os.path.join(path, pipeline_file)).readlines():
            line = line.strip()
            if not line or line[0] == '#':
                continue
            print '  ', line
    print


class RuntimeConfig(object):

    """Class that manages the configuration settings. This includes keeping track of the
    language, the source directory or file, pipeline configuration settings etcetera. The
    setting in here are particular to a certain pipeline as defined for a corpus."""
    
    def __init__(self, target_path, language, pipeline_config_file):
        self.target_path = target_path
        self.language = language
        self.config_dir = os.path.join(target_path, 'config')
        self.general_config_file = os.path.join(self.config_dir, 'general.txt')
        self.pipeline_config_file = os.path.join(self.config_dir, pipeline_config_file)
        self.filenames = os.path.join(self.config_dir, 'files.txt')
        self.general = {}
        self.pipeline = []
        self.read_general_config()
        self.read_pipeline_config()

    def read_general_config(self):
        for line in open(self.general_config_file):
            (var, val) = line.strip().split('=')
            self.general[var.strip()] = val.strip()
            if var.strip() == 'language':
                self.language = val.strip()

    def read_pipeline_config(self):
        self.pipeline = read_pipeline_config(self.pipeline_config_file)

    def source(self):
        source = self.source_path()
        return source if source is not None else self.source_file()
 
    def source_path(self):
        path = self.general['source_path']
        return None if path == 'None' else path
    
    def source_file(self):
        filename = self.general['source_file']
        return None if filename == 'None' else filename

    def get_options(self, stage):
        for step in self.pipeline:
            if step[0] == stage:
                return step[1]
        print "[get_options] WARNING: processing stage not found"
        return {}

    def pp(self):
        print "\n<GlobalConfig on '%s'>" % (self.target_path)
        print "\n   General Config Settings"
        for k,v in self.general.items():
            print "      %s ==> %s" % (k,v)
        print "\n   Pipeline Config Settings"
        for k,v in self.pipeline:
            print "      %s ==> %s" % (k,v)
        print
        

class DataSet(object):

    """
    Instance variables:

       type:
          name of the directory in 'data' where all files are

       version_id:
           subdir in the output, None for the --populate stage (??)

       stage_name:
          name of the stage creating files in the data set, this name is not
          always specified and can be None, but it will always be set when a
          DataSet is created in the context of a processing stage.

       output_name2:
          optional second directory for output files

       global_config: global configuration settings handed over by the
          environment, these do not necessary match anything in the dataset, in
          fact, checking whether the internals match the global config is the
          way to determine whether a data set is relevant for a particular
          pipeline.  """

    def __init__(self, stage_name, output_name, config, id='01'):
        self.type = output_name
        self.version_id = id
        self.stage_name = stage_name
        self.files_processed = 0
        self.global_config = config
        self.local_config = None
        self.pipeline_head = None
        self.pipeline_trace = None
        self.base_path = os.path.join(config.target_path, 'data')
        self.path = os.path.join(self.base_path, self.type, self.version_id)
        if self.exists():
            self.load_from_disk()

    def __str__(self):
        return "<DataSet %s version_id=%s files=%d>" % \
            (self.type, self.version_id, self.files_processed)
    
    def initialize_on_disk(self):
        """All that is guaranteed to exist is a directory like data/patents/en/d1_txt, but
        sub structures is not there. Create the substructure and initial versions of all
        needed files in configuration and state directories."""
        for subdir in ('config', 'state', 'files'):
            ensure_path(os.path.join(self.path, subdir))
        create_file(os.path.join(self.path, 'state', 'processed.txt'), "0\n")
        create_file(os.path.join(self.path, 'state', 'processing-history.txt'))
        trace, head = self.split_pipeline()
        trace_str = pipeline_component_as_string(trace)
        head_str = pipeline_component_as_string([head])
        create_file(os.path.join(self.path, 'config', 'pipeline-head.txt'), head_str)
        create_file(os.path.join(self.path, 'config', 'pipeline-trace.txt'), trace_str)
        self.files_processed = 0
        
    def split_pipeline(self):
        """Return a pair of pipeline trace and pipeline head from the config.pipeline
        given the current processing step in self.stage_name."""
        trace = []
        for step in self.global_config.pipeline:
            if step[0] == self.stage_name:
                return trace, step
            else:
                trace.append(step)
        print "WARNING: did not find processing step in pipeline"
        return None
        
    def get_options(self):
        return self.global_config.get_options(self.stage_name)
            
    def load_from_disk(self):
        """Get the state and the local configuration from the disk. Does not need to get the
        processing history since all we need to do to it is to append that information
        from the latest processing step."""
        fname1 = os.path.join(self.path, 'state', 'processed.txt')
        fname2 = os.path.join(self.path, 'config', 'pipeline-head.txt')
        fname3 = os.path.join(self.path, 'config', 'pipeline-trace.txt')
        self.pipeline_head = read_pipeline_config(fname2)[0]
        self.pipeline_trace = read_pipeline_config(fname3)
        self.files_processed = int(open(fname1).read().strip())
    
    def exists(self):
        """Return True if the data set exists on disk, False otherwise."""
        return os.path.exists(self.path)

    def update_state(self, limit, t1):
        """Update the content of state/processed.txt and state/processing-history.txt."""
        # TODO: should not just print the files processed in the history, but also the
        # range of files.
        time_elapsed =  time.time() - t1
        processed = "%d\n" % self.files_processed
        create_file(os.path.join(self.path, 'state', 'processed.txt'), processed)
        history_file = os.path.join(self.path, 'state', 'processing-history.txt')
        fh = open(history_file, 'a')
        fh.write("%d\t%s\t%s\t%s\n" % (limit, time.strftime("%Y:%m:%d-%H:%M:%S"),
                                       get_git_commit(), time_elapsed))

    def input_matches_global_config(self):
        """This determines whether the data set matches the global pipeline configuration
        if the data set is considered to be the input to the current processing step. This
        amounts to checking whether match dataset.trace + dataset.head is equal to
        global_config.pipeline(txt).trace."""
        gc_trace, gc_head = self.split_pipeline()
        ds_trace_plus_head = self.pipeline_trace
        ds_trace_plus_head.append(self.pipeline_head)
        #print 'gc_trace            -- ', gc_trace
        #print 'ds_trace_plus_head  -- ', ds_trace_plus_head
        return ds_trace_plus_head == gc_trace

    def output_matches_global_config(self):
        """This determines whether the data set matches the global pipeline configuration
        if the data set is considered to be the output to the current processing
        step. This amounts to checking whether match dataset.trace + dataset.head is equal
        to global_config.pipeline(txt).trace."""
        gc_trace, gc_head = self.split_pipeline()
        gc_trace_plus_head = gc_trace
        gc_trace_plus_head.append(gc_head)
        ds_trace_plus_head = self.pipeline_trace
        ds_trace_plus_head.append(self.pipeline_head)
        #print 'gc_trace_plus_head  -- ', gc_trace_plus_head
        #print 'ds_trace_plus_head  -- ', ds_trace_plus_head
        return ds_trace_plus_head == gc_trace_plus_head
    
    def pp(self):
        """Simplistic pretty print."""
        print "\n%s\n" % self
        print "   stage_name:",  self.stage_name
        print "   global_config.pipeline"
        for e in self.global_config.pipeline:
            print "     ", e[0], e[1]
        print "   pipeline_head"
        print "     ", self.pipeline_head[0], self.pipeline_head[1]
        print "   pipeline_trace"
        for e in self.pipeline_trace:
            print "     ", e[0], e[1]
        print
