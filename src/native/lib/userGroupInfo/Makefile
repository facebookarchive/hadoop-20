CC=gcc
CFLAGS=-lc -shared -fPIC

TARGET=libGetUserGroupInfo.so
SOURCES=org_apache_hadoop_hdfs_nfs_nfs4_UserIDMapperSystem.c
INCLUDES=-I/usr/local/jdk-7u10-64/include -I/usr/local/jdk-7u10-64/include/linux


all:
	$(CC) $(CFLAGS) $(INCLUDES) $(SOURCES) -o $(TARGET)

clean:
	rm -rf $(TARGET)
