CC = g++
CXX = g++
AR = ar
ARFLAGS = rcs
RM = rm -f
RANLIB = ranlib

INCS=-I.
CFLAGS = -Wall -std=c++11 -g $(INCS)
CXXFLAGS = -Wall -std=c++11 -g $(INCS)

SRC = MapReduceFramework.cpp
OBJ = $(SRC:.cpp=.o)
LIB = libMapReduceFramework.a

all: $(LIB)
# archive the object into a .a and index it
$(LIB): $(OBJ)
	$(AR)   $(ARFLAGS) $@ $^
	$(RANLIB) $@

# compile rule for .cpp → .o
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

.PHONY: clean
clean:
	$(RM) $(OBJ) $(LIB) *~ core

TARNAME = ex3.tar
TARSRCS = $(SRC) Makefile README

.PHONY: tar
tar: clean all
	tar -cvf $(TARNAME) $(TARSRCS)



