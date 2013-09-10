CC=gcc
CFLAGS=-I. -Wall -W -Wundef -Wno-implicit-function-declaration -fPIC -g
LDFLAGS      = -shared

OS := $(shell uname)
ifeq ($(OS),Linux)
EXT =
else
EXT =.exe
endif

TARGET = liblz4.so
SOURCES = lz4.c lz4hc.c
OBJECTS = $(SOURCES:.c=.o)

default: lz4c

all: $(TARGET) lz4c

$(TARGET): $(OBJECTS)
	$(CC) $(FLAGS) $(CFLAGS) $(LDFLAGS) -o $(TARGET) $(OBJECTS)

lz4c: bench.c xxhash.c lz4c.c
	$(CC) -O3 $(CFLAGS) $^ -o $@$(EXT) -L. -llz4

clean:
	rm -f core *.o lz4c$(EXT) *.so

