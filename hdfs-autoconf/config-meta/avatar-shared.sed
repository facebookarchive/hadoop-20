# setting up shared avatar directories
# all these paths will be created relatively to /tmp directory
s:{{NameNode-shared}}:/tmp/hadoop-avatar-shared/:g
s:{{NameNode-shared-fsimage-0}}:/tmp/hadoop-avatar-shared/fsimage-zero/:g
s:{{NameNode-shared-fsedits-0}}:/tmp/hadoop-avatar-shared/fsedits-zero/:g
s:{{NameNode-shared-fsimage-1}}:/tmp/hadoop-avatar-shared/fsimage-one/:g
s:{{NameNode-shared-fsedits-1}}:/tmp/hadoop-avatar-shared/fsedits-one/:g

# ground may be a separator as well
s_{{zookeeper-quorum}}_localhost_g
