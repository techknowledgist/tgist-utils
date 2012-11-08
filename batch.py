
def read_stages(target_path, language):
    stages = {}
    for line in open(os.path.join(target_path, language, 'ALL_STAGES.txt')):
        if not line.strip():
            continue
        (stage, count) = line.strip().split("\t")
        stages[stage] = int(count)
    return stages

def update_stages(target_path, language, stage, limit):
    """Updates the counts in ALL_STAGES.txt. This includes rereading the file because
    during processing on one machine another machine could have done some other processing
    and have updated the fiel, we do not want to lose those updates. This could
    potentially go wrong when two separate processes terminate at the same time, a rather
    unlikely occurrence."""
    stages = read_stages(target_path, language)
    stages.setdefault(stage, 0)
    stages[stage] += limit
    write_stages(target_path, language, stages)
    
def write_stages(target_path, language, stages):
    stages_file = os.path.join(target_path, language, 'ALL_STAGES.txt')
    backup_file = os.path.join(target_path, language,
                               "ALL_STAGES.%s.txt" % time.strftime("%Y%m%d-%H%M%S"))
    shutil.copyfile(stages_file, backup_file)
    fh = open(stages_file, 'w')
    for stage, count in stages.items():
        fh.write("%s\t%d\n" % (stage, count))
    fh.close()
