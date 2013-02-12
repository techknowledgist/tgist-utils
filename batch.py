"""

Various utilities for ontology creation and pattern matcher.

"""

import os, time, shutil

from ontology.utils.file import ensure_path, get_lines, create_file
from ontology.utils.git import get_git_commit


def read_pipeline_config(pipeline_file):
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


class GlobalConfig(object):

    """Class that manages the configuration settings. This includes keeping track of the
    language, the source directory or file, pipeline configuration settings etcetera."""
    
    def __init__(self, target_path, language, pipeline_config_file):
        self.target_path = target_path
        self.language = language
        config_dir = os.path.join(target_path, language, 'config')
        self.general_config_file = os.path.join(config_dir, 'general.txt')
        self.pipeline_config_file = os.path.join(config_dir, pipeline_config_file)
        self.filenames = os.path.join(config_dir, 'files.txt')
        self.general = {}
        self.pipeline = []
        self.read_general_config()
        self.read_pipeline_config()

    def read_general_config(self):
        for line in open(self.general_config_file):
            (var, val) = line.strip().split('=')
            self.general[var.strip()] = val.strip()
        
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
        print "\n<GlobalConfig on '%s/%s'>" % (self.target_path, self.language)
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

       stage_name - name of the stage creating files in the data set

       version_id - subdir in the output, None for the --populate stage (??)

       output_name1 - name of the directory where files are created

       output_name2 - optional second directory for output files

       global_config - global configuration settings handed over by the envirnoment, these
          do not necessary match anything in the dataset, in fact, checking whether the
          internals match the global config is the way to determine whether a data set is
          relevant for a particular pipeline.

          
    """

    @classmethod
    def pipeline_component_as_string(cls, trace):
        elements = []
        for element in trace:
            elements.append(element[0] + " " +
                            " ".join(["%s=%s" % (k,v) for k,v in element[1].items()]))
        return "\n".join(elements).strip() + "\n"
    
    
    def __init__(self, stage_name, output_names, config, id='01'):
        self.stage_name = stage_name
        self.version_id = id
        self.output_name1 = output_names[0]
        self.output_name2 = output_names[1] if len(output_names) > 1 else None
        self.files_processed = 0
        self.global_config = config
        self.local_config = None
        self.pipeline_head = None
        self.pipeline_trace = None
        self.base_path = os.path.join(config.target_path, config.language, 'data')
        self.path1 = os.path.join(self.base_path, self.output_name1, self.version_id)
        self.path2 = None
        if self.output_name2 is not None:
            self.path2 = os.path.join(self.base_path, self.output_name2, self.version_id)
        if self.exists():
            self.load_from_disk()


    def __str__(self):
        return "<DataSet on '%s' exists=%s processed=%d>" % (self.path1, self.exists(),
                                                             self.files_processed)
    
    def initialize_on_disk(self):
        """All that is guaranteed to exist is a directory like data/patents/en/d1_txt, but sub
        structures is not there. Create the substructure and initial versions of all
        needed files in configuration and state directories."""
        for subdir in ('config', 'state', 'files'):
            ensure_path(os.path.join(self.path1, subdir))
        if self.path2 is not None:
            for subdir in ('config', 'state', 'files'):
                ensure_path(os.path.join(self.path2, subdir))
        create_file(os.path.join(self.path1, 'state', 'processed.txt'), "0\n")
        create_file(os.path.join(self.path1, 'state', 'processing-history.txt'))
        if self.path2 is not None:
            create_file(os.path.join(self.path2, 'state', 'processed.txt'), "0\n")
            create_file(os.path.join(self.path2, 'state', 'processing-history.txt'))
        trace, head = self.split_pipeline()
        trace_str = DataSet.pipeline_component_as_string(trace)
        head_str = DataSet.pipeline_component_as_string([head])
        create_file(os.path.join(self.path1, 'config', 'pipeline-head.txt'), head_str)
        create_file(os.path.join(self.path1, 'config', 'pipeline-trace.txt'), trace_str)
        if self.path2 is not None:
            create_file(os.path.join(self.path2, 'config', 'pipeline-head.txt'), head_str)
            create_file(os.path.join(self.path2, 'config', 'pipeline-trace.txt'), trace_str)
        self.files_processed = 0
        
    def split_pipeline(self):
        """Return a pair of pipeline trace and pipeline head from the config.pipeline given the
        current processing step in self.stage_name."""
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
        fname1 = os.path.join(self.path1, 'state', 'processed.txt')
        fname2 = os.path.join(self.path1, 'config', 'pipeline-head.txt')
        fname3 = os.path.join(self.path1, 'config', 'pipeline-trace.txt')
        self.pipeline_head = read_pipeline_config(fname2)[0]
        self.pipeline_trace = read_pipeline_config(fname3)
        self.files_processed = int(open(fname1).read().strip())
    
    def exists(self):
        """Return True is the data set exists on disk, False otherwise."""
        return os.path.exists(self.path1)

    def update_state(self, limit, t1):
        """Update the content of state/processed.txt and state/processing-history.txt."""
        time_elapsed =  time.time() - t1
        processed = "%d\n" % self.files_processed
        create_file(os.path.join(self.path1, 'state', 'processed.txt'), processed)
        history_file = os.path.join(self.path1, 'state', 'processing-history.txt')
        fh = open(history_file, 'a')
        fh.write("%d\t%s\t%s\t%s\n" % (limit, time.strftime("%Y:%m:%d-%H:%M:%S"),
                                       get_git_commit(), time_elapsed))


    def input_matches_global_config(self):
        """This determines whether the data set matches the global pipeline configuration if the
        data set is considered to be the input to the current processing step. This
        amounts to checking whether match dataset.trace + dataset.head is equal to
        global_config.pipeline(txt).trace."""
        gc_trace, gc_head = self.split_pipeline()
        ds_trace_plus_head = self.pipeline_trace
        ds_trace_plus_head.append(self.pipeline_head)
        #print 'gc_trace            -- ', gc_trace
        #print 'ds_trace_plus_head  -- ', ds_trace_plus_head
        return ds_trace_plus_head == gc_trace
    

    def output_matches_global_config(self):
        """This determines whether the data set matches the global pipeline configuration if the
        data set is considered to be the output to the current processing step. This
        amounts to checking whether match dataset.trace + dataset.head is equal to
        global_config.pipeline(txt).trace."""
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
        if self.path2 is not None:
            print "    path2: %s" % self.path2
        print "   global_config.pipeline"
        for e in self.global_config.pipeline:
            print "     ", e[0], e[1]
        print "   pipeline_head"
        print "     ", self.pipeline_head[0], self.pipeline_head[1]
        print "   pipeline_trace"
        for e in self.pipeline_trace:
            print "     ", e[0], e[1]
        print
        

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
    # working on (eg en/tag), (ii) using the pipeline configuration to find what
    # subdirectory to use, (iii) reading the local stages file in there (which might now
    # be called something like processing-dribble or what not)
    
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
